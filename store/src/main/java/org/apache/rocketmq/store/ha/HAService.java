/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * RocketMQ 主从同步核心实现类
 * todo 注意：该类 Master 和 Slave 都会使用，具体根据不同的角色发挥不同的作用。
 * <p>
 * RocketMQ HA 机制大体可以分为以下三个部分：
 * 1. Master 启动并监听 Slave 的连接
 * 2. Slave 启动，主动与 Master 建立连接，Master 接收 Slave 的连接，并建立相关 TCP 连接
 * 3. Slave 以每隔 5s 的间隔时间发送待拉取偏移量，等待 Master 返回数据
 * 4. Slave 保存消息并继续发送新的消息同步请求
 * todo 主从复制的关键两大步骤：
 * 1 复制工作是由 HAConnection 和 HAClient 完成的。具体是：
 * - 1.1  HAService 启动。根据当前服务器的角色，可能是 Master ，也可能是 Slave
 * - 1.2  Master 会打开通道，监听 Slave 的连接。一旦有 Slave 连接就会封装建立的通道，并启动两个线程任务，分别用于读取从服务器复制消息请求、向从服务器发送复制的消息
 * - 1.3  Slave 会启动线程任务，完成连接 Master、向 Master 上报自己的复制偏移量（向 Master 发送复制请求）、处理 Master 回传的消息并追加到 commitlog 文件中
 * 以上三个过程不停执行，完成 Slave 和 Master 的交互，是想主从复制
 * <p>
 * 2 消息发送者执行主从复制
 * - 2.1 这个和真正的主从复制没有关系，严格说这个是判断主从复制是否完成的逻辑。因为主从复制是另一个逻辑完成的。
 * - 2.2 GroupTransferService 用于实现消息生产方等待主从复制完成或超时
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * Master 维护的连接数（Slave 的个数）
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    /**
     * 具体连接信息，是对 主从服务器的连接 SocketChannel 的封装
     */
    private final List<HAConnection> connectionList = new LinkedList<>();
    /**
     * 实现主服务器监听从服务器的连接 - 线程任务
     */
    private final AcceptSocketService acceptSocketService;
    /**
     * Broker 存储实现
     */
    private final DefaultMessageStore defaultMessageStore;
    /**
     * 等待-通知对象
     */
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    /**
     * 该 Master 所有 Slave 中同步最大的偏移量
     * todo 非常重要，该属性会周期性地根据主从复制情况进行更新。是作为判断是否复制成功的参考量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 主从复制线程任务
     */
    private final GroupTransferService groupTransferService;

    /**
     * HA 客户端实现，Slave 端网络的实现类
     */
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
                new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
                result
                        && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                        .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 通知主从复制
     * 该方法是在主服务器收到从服务器的拉取请求后被调用。
     * todo 本质上是通过更新 push2SlaveMaxOffset 的值来达到唤醒阻塞等待复制完成的消息发送者线程，因为判断是否完成复制的依据是 预期复制点 <= push2SlaveMaxOffset
     *
     * @param offset Slave 下一次待拉取的消息偏移量，也可以认为是 Slave 的拉取偏移量确认信息
     */
    public void notifyTransferSome(final long offset) {
        // 如果 Slave 下一次待拉取的消息偏移量大于 push2SlaveMaxOffset,则更新 push2SlaveMaxOffset
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 唤醒 GroupTransferService 任务线程
                this.groupTransferService.notifyTransferSome();
                break;

                // 更新失败，则重试
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }


    /**
     * HAServer 启动流程
     * todo  注意：不管是 Master 还是 Slave 都将按照上述流程启动，在内部的实现会根据 Broker 配置来决定真正开启的流程
     *
     * @throws Exception
     */
    public void start() throws Exception {
        // 1 开启服务端监听，用于处理 Slave 客户端的连接请求。并启动对应的监听线程，处理监听逻辑。
        // todo 针对 Master 角色
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();

        // 2 启动复制线程任务
        this.groupTransferService.start();

        // 3 启动 HA 客户端线程
        // todo 针对 Slave 角色
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * 实现 Master 端监听 Slave 连接的实现类。
     * 说明：将主服务器和从服务器的连接封装为 HAConnection，并加入到 HAServer 中维护的缓存中
     * <p>
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        /**
         * Broker 服务监听套接字（本地IP + 端口号）地址
         */
        private final SocketAddress socketAddressListen;
        /**
         * 服务端 Socket 通道，基于 NIO
         */
        private ServerSocketChannel serverSocketChannel;
        /**
         * 事件选择器，基于 NIO
         */
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 打开 服务端 Socket 通道
            this.serverSocketChannel = ServerSocketChannel.open();

            // 创建 Selector
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);

            // 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);

            // 设置为非阻塞模式
            this.serverSocketChannel.configureBlocking(false);

            // 注册连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 该方法是标准的基于 NIO 的服务端程序实例。
         * 1 选择器每 1s 处理一次连接事件
         * 2 连接事件就绪后，调用 ServerSocketChannel 的 accept() 方法创建 SocketChannel
         * 3 为每个连接创建一个 HAConnection 对象，该 HAConnection 将负责主从数据同步逻辑
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {

                    // 1 每 1s 处理一次连接就绪事件
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {

                                // 2 连接事件就绪后，创建 SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                            + sc.socket().getRemoteSocketAddress());

                                    try {

                                        // 3 todo 创建与服务端数据传输的通道，该对象负责主从数据同步逻辑。是对连接通道的封装
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // todo 创建后立即启动 HAConnection 的两大任务：
                                        // 1 启动向从服务器写入数据的线程
                                        // 2 启动从从服务器读取数据的线程
                                        // 这两个线程会不断轮询
                                        conn.start();

                                        // 4 在 HAServer 中维护 HAConnection
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * todo 同步复制任务，即GroupTransferService 的职责就是判断主从同步是否结束，而非主从同步逻辑
     * 说明：
     * 1 主从同步阻塞实现，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器
     * 2 todo 从服务器数据的复制是 HAConnection 中的逻辑，具体通过开启两个线程实现的。所以消息发送者在这里需要等待数据传输的结果，即这里去判断 HAConnection 中的结果
     * 3 todo 该类的整体结构与同步刷盘实现类类似
     * <p>
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();

        /**
         * 写同步复制队列
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();

        /**
         * 读同步复制队列
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * 接收同步复制请求
         *
         * @param request
         */
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }

            // 唤醒，立即执行复制操作
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        /**
         * 交换读写复制队列
         */
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 消息发送者线程提交任务后将被阻塞直到超时或 GroupTransferService 通知唤醒。todo 也就是 GroupTransferService 的职责就是判断主从同步是否结束。
         * <p>
         * 1 GroupTransferService 负责在主从同步复制结束后，通知由于等待同步结果而阻塞的消息发送者线程。
         * 2 todo 判断主从同步是否完成的依据：从服务器中已成功复制的消息最大偏移量是否大于、等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量(也就是预期的复制点)。
         * 2.1 如果是，则表示主从同步复制已经完成，唤醒消息发送线程。
         * 2.2 否则，等待 1s 再次判断，每个请求会循环判断 5 次。
         * 3 消息发送者返回有两种情况：
         * 3.1 等待超过 5s
         * 3.2 GroupTransferService 通知主从复制完成
         */
        private void doWaitTransfer() {
            // 对 对同步复制队列 加锁，顺序处理消息发送者线程提交的主从复制是否成功的查询请求
            synchronized (this.requestsRead) {

                if (!this.requestsRead.isEmpty()) {

                    // 遍历读同步队列，其实是【查询复制是否完成的请求】
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {

                        // todo 判断是否完成主从同步
                        // 从服务器中已成功复制的消息最大偏移量是否大于、等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，也就预计复制点
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();

                        // 同步复制的超时时间，默认是 5s，作为每个任务最多循环判断次数
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                                + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();


                        // 循环检测是否完成复制
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // 每检测一次休眠 1s
                            this.notifyTransferObject.waitForRunning(1000);

                            // todo 再次判断是否已成功复制
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // todo 根据复制结果，尝试唤醒等待的线程
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    // 每次处理多有同步请求后，清理读队列。用于存储下次待处理同步复制请求（todo 本质上是查询同步复制是否完成的请求，真正地同步复制请求不在这里处理）
                    this.requestsRead.clear();
                }
            }
        }

        /**
         * 查询复制是否完成
         */
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 等待 10ms
                    this.waitForRunning(10);

                    // 查询复制是否完成
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA 客户端实现
     * todo 从服务端的核心实现类，即专门用于从服务器
     */
    class HAClient extends ServiceThread {

        /**
         * Socket 读缓存区大小
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * 主服务器地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**
         * 从服务器向主服务器发起主从同步的拉取偏移量，固定 8 个字节
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /**
         * 网络传输通道
         */
        private SocketChannel socketChannel;
        /**
         * NIO 事件选择器
         */
        private Selector selector;
        /**
         * 上一次写入消息的时间戳，todo 向 主服务器写？
         */
        private long lastWriteTimestamp = System.currentTimeMillis();
        /**
         * 反馈到主服务器的当前从服务器的复制进度，即 CommitLog 文件的最大偏移量
         */
        private long currentReportedOffset = 0;
        /**
         * 本次已处理读缓存区的指针，即 byteBufferRead 中已转发的指针。
         */
        private int dispatchPosition = 0;

        /**
         * 读缓存区，大小为 4M
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 读缓存区备份，与 byteBufferRead 进行替换。
         * todo 主要用于 处理 byteBufferRead 中未包含一条完整的消息的处理逻辑
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 是否向主服务器反馈已拉取消息偏移量
         * todo 依据：如果超过 5s 的间隔时间没有请求拉取则反馈。
         *
         * @return
         */
        private boolean isTimeToReportOffset() {

            // 距离上次写入消息的时间戳
            long interval =
                    HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;

            // 主服务器与从服务器的高可用心跳发送时间间隔默认为 5s
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                    .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向主服务器反馈当前拉取偏移量。这里有两重含义：
         * 1 对于从服务器来说，是向主服务器发送下次待拉取消息的偏移量
         * 2 对于主服务器来说，既可以认为是从服务器本次请求拉取的消息偏移量，也可以理解为从服务器的消息同步 ACK 确认消息
         * todo 结果：向主服务器发送一个8字节的请求，请求包中包含的数据为从服务器消息文件的最大偏移量。
         *
         * @param maxOffset 从服务器当前的复制进度，一般都是消息的最大偏移量
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {

            // 将偏移量写入到 ByteBuffer 中
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);

            // 将 ByteBuffer 从写模式切换到读模式，以便将数据传入通道
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 将一个 ByteBuffer 写入到通道，通常使用循环写入
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {

                    // todo 发送到主服务器
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                            + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            // 更新上次写消息的时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }


        /**
         * 处理 byteBufferRead 中未包含一条完整的消息的处理逻辑。
         * todo 核心思想：将readByteBuffer中剩余的有效数据先复制到readByteBufferBak,然后交换readByteBuffer与readByteBufferBak，以便更多数据到达再处理。
         */
        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理网络读请求
         * 说明：处理从主服务器传回的消息数据
         *
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // 循环判断 byteBufferRead 读缓存区是否还有剩余空间
            while (this.byteBufferRead.hasRemaining()) {
                try {

                    // 如果存在剩余空间，则调用 SocketChannel.read 方法将通道中的数据读入读缓存区 byteBufferRead
                    int readSize = this.socketChannel.read(this.byteBufferRead);

                    // 如果读取到的字节数 > 0
                    if (readSize > 0) {

                        // 重置读取到 0 字节的次数
                        readSizeZeroTimes = 0;

                        // todo 将读取到的所有消息全部追加到消息内存映射文件中，然后反馈拉取进度给主服务器
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }

                        // 如果读取到字节数 == 0 ，则累加读取到 0 字节的次数
                    } else if (readSize == 0) {
                        // 如果连续3次从网络通道读取到0个字节，则结束本次读，返回true。
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }

                        // 如果读取到的字节数小于0，则返回 0
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }

                    // 发生IO异常，则返回false
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * todo 将主服务器回传的消息追加到 Commitlog 中并转发到消息消费队列与索引文件中。
         * 1 将主服务器传回的消息追加到消息内存映射文件中，
         * todo 即该方法主要从 byteBufferRead 读缓存区中解析一条一条的消息，然后存储到 CommitLog 文件并转发到消息消费队列和索引文件中
         * 2 如何判断 byteBufferRead 是否包含一条完整的消息
         *
         * @return
         */
        private boolean dispatchReadRequest() {

            // 1 定义头部长度，大小为 12 个字节，包括消息的物理偏移量与消息的长度
            // 注意：长度字节必须首先探测，否则无法判断byteBufferRead缓存区中是否包含一条完整的消息
            final int msgHeaderSize = 8 + 4; // phyoffset + size

            // 2 记录当前 byteBufferRead 读缓存区的当前指针
            int readSocketPos = this.byteBufferRead.position();

            while (true) {

                // 3 先探测 byteBufferRead 缓冲区中是否包含一条消息的头部，如果包含头部，则读取物理偏移量与消息长度，然后再探测是否包含一条完整的消息，如果不包含，
                // 则需要将byteBufferRead中的数据备份，以便更多数据到达再处理。

                // 3.1 剩余没有处理的缓存区
                int diff = this.byteBufferRead.position() - this.dispatchPosition;

                // 3.2 读缓存区是否包含一条消息头部
                // todo 主服务器发送信息分为两部分：头 + 体
                if (diff >= msgHeaderSize) {

                    // 读取 8 字节的消息物理偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    // 读取 4 字节的消息的长度
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 3.3 获取从服务器当前消息文件的最大物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    // 3.4 如果slave的最大物理偏移量与master给的偏移量不相等，则返回false
                    // 注意：从后面的处理逻辑来看，返回false,将会关闭与master的连接，在Slave本次周期内将不会再参与主从同步了。
                    if (slavePhyOffset != 0) {

                        // todo 要和发送过去的偏移量对应，以保持连续
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                    + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }


                    // 探测是否包含一条完整的消息，包含的话就可以将消息内容追加到消息内存映射文件中了。
                    if (diff >= (msgHeaderSize + bodySize)) {

                        // 创建消息体大小的字节数组
                        byte[] bodyData = new byte[bodySize];

                        // 设置 byteBufferRead 的 position 指针为 this.dispatchPosition + msgHeaderSize
                        // todo 表示 byteBufferRead 已转发的进度
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);

                        // 读取消息体的长度的字节内容到 bodyData 中
                        this.byteBufferRead.get(bodyData);

                        // todo 将消息内容追加到消息内存映射文件中
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);


                        this.byteBufferRead.position(readSocketPos);

                        // 更新 dispatchPosition
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // todo 向主服务端及时反馈当前已存储进度，也就是继续请求复制消息请求
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                // todo 如果byteBufferRead中未包含一条完整的消息，则需要将byteBufferRead中的数据备份，以便更多数据到达再处理。
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }
                break;
            }

            return true;
        }

        /**
         * 尝试向主服务器反馈从服务器当前已存储进度。
         *
         * @return
         */
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            // 获取从服务器 CommitLog 最大偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

            // 如果大于内存中记录从服务器当前的复制进度 currentReportedOffset ，则更新 currentReportedOffset
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;

                // 向主服务器反馈 从服务器当前的复制进度 currentReportedOffset
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 连接主服务器
         * 说明：
         * 在 Broker 启动时，如果 Broker 的角色为从服务器，则读取 Broker 配置文件中的 haMasterAddress 属性并更新 HAClient 的 masterAddress ，
         * 如果角色为从服务器，但 haMasterAddress 为空，启动 Broker 不会报错，但不会执行主从同步复制。
         *
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            // 1 如果 socketChannel 为 null，则尝试连接主服务器。
            if (null == socketChannel) {
                String addr = this.masterAddress.get();

                // 如果主服务器地址不为空，则建立到主服务器的 TCP 连接
                if (addr != null) {
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);

                        // todo 注册 OP_READ (网络读事件)，用于接收主服务器回传的消息
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                // 初始化从服务器当前的复制进度，即 CommitLog 文件的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                // 上一次写入消息的时间初始化为当前时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * todo 该方法是 HAClient 整个工作机制的实现
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {

                    // 1 从服务器连接主服务器，处于连接状态就不需要重新连接。
                    // todo 连接主服务器
                    if (this.connectMaster()) {

                        // 2 判断是否需要向主服务器反馈当前已经拉取消息偏移量，
                        if (this.isTimeToReportOffset()) {

                            // 3 如果需要向主服务器反馈拉取消息偏移量，则向主服务器发送一个 8 字节的请求，请求包中包含的数据为当前 Broker（从服务器） 消息文件的最大偏移量。首次是 0
                            // todo 反馈也是向主服务器发送复制请求，请求偏移量每次都是从服务器的消息文件的最大偏移量
                            // todo 发送复制请求
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);

                            // 发送失败，则管理连接，下次重新建立
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 4 进行事件选择，执行间隔时间为 1s
                        this.selector.select(1000);

                        // 5 todo 处理网络读请求，即处理从主服务器传回的消息数据
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 6 尝试向主服务器反馈当前已存储 CommitLog 的偏移量，在需要的时候更新 currentReportedOffset
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                                HAService.this.getDefaultMessageStore().getSystemClock().now()
                                        - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                                .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                    + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }


                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
