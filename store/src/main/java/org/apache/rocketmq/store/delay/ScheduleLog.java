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
package org.apache.rocketmq.store.delay;

import io.netty.util.HashedWheelTimer;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.delay.wheel.ScheduleThreadFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScheduleLog {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    // End of file empty MAGIC CODE cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;

    /**
     * ConsumeQueue 条目 存储在 MappedFile 的内容必须大小是 20 字节
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    /**
     * RocketMQ 存储核心服务
     */
    private final ScheduleMessageStore defaultMessageStore;

    /**
     * ConsumeQueue 对应的 MappedFile 的队列
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * 存储路径：rockemt_home/store/consume/ {topic} / {queryId}
     */
    private final String storePath;

    /**
     * 完整路径
     */
    private String scheduleDir = "";
    /**
     * 默认大小为，30W条记录，也就是30W * 20字节。
     */
    private final int mappedFileSize;
    /**
     * 记录当前 ConsumeQueue 中存放的消息索引对象消息的最大物理偏移量（是在 CommitLog 中）
     * todo 该属性主要作用是判断当前 ConsumeQueue 已经保存消息索引对应消息的物理偏移量，和 ConsumeQueue 物理偏移量没有关系
     *
     *  ScheduleLog#recover()  通过 ConsumeQueue 计算得来的，后续随着重放消息进行更新
     */
    private long maxPhysicOffset = -1;

    private ConsumeQueueExt consumeQueueExt = null;

    private HashedWheelTimer hashedWheelTimer;

    /**
     * 刷盘任务，根据刷盘方式，可能是 同步刷盘，也可能是异步刷盘
     */
    private final FlushCommitLogService flushCommitLogService;

    /**
     * 创建并初始化消息队列
     *
     * @param dirMills
     * @param storePath
     * @param mappedFileSize
     * @param defaultMessageStore
     */
    public ScheduleLog(
            final long dirMills,
            final String storePath,
            final int mappedFileSize,
            final ScheduleMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;
        /**
         * 创建 MappedFile 的队列
         */
        this.mappedFileQueue = new MappedFileQueue(this.storePath + File.separator + dirMills, mappedFileSize, null);

        // 格式：.../store/schedulelog/1657276200000
        this.scheduleDir = this.storePath + File.separator + dirMills;

        // FIXME 这个版本没有统计 HashedWheelTimer 个数，如果有的话可以在达到阈值，剩下的使用共享时间轮
        this.hashedWheelTimer = new ScheduleTimeWheel().getWheelTimer();

        // 根据刷盘方式，同步刷盘使用 GroupCommitService
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();

            // 异步刷盘使用 FlushRealTimeService
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        // todo 启动刷盘
        this.flushCommitLogService.start();

    }

    /**
     * 每个 ScheduleLog 绑定一个时间轮
     */
    class ScheduleTimeWheel {
        private final HashedWheelTimer wheelTimer;

        {
            wheelTimer = new HashedWheelTimer(
                    new ScheduleThreadFactory(ScheduleLog.this.scheduleDir),
                    1000,
                    TimeUnit.MILLISECONDS, 128);
        }

        public HashedWheelTimer getWheelTimer() {
            return wheelTimer;
        }
    }


    /**
     * 刷盘任务
     * 说明：MappedFile 落盘方式：
     * 1 开启使用堆外内存：数据先追加到堆外内存 -> 提交堆外内存数据到与物理文件的内存映射中 -> 物理文件的内存映射 flush 到磁盘
     * 2 没有开启使用堆外内存：数据直接追加到与物理文件直接映射的内存中 -> 物理文件的内存映射 flush 到磁盘
     */
    abstract static class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }


    /*-------------------------------- 异步刷盘 -----------------------------*/

    /**
     * 异步刷盘 && 关闭内存字节缓冲区
     * 说明：实时 flush 线程服务，调用 MappedFile#flush 相关逻辑
     */
    public class FlushRealTimeService extends FlushCommitLogService {
        // 最后 flush 的时间戳
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        /**
         * 休眠等待 或 超时等待唤醒 进行刷盘
         */
        @Override
        public void run() {
            ScheduleLogManager.log.info(this.getServiceName() + " service started");

            // Broker 不关闭
            while (!this.isStopped()) {

                // 任务执行的时间间隔，默认 500ms
                int interval = 500;

                // 一次刷盘任务至少包含页数，如果待刷盘数据不足，小于该参数配置的值，将忽略本次刷盘任务，默认 4 页
                // todo 调成 0 ，立即刷盘
                int flushPhysicQueueLeastPages = 4;

                // 两次真实刷盘的最大间隔时间，默认 10s
                int flushPhysicQueueThoroughInterval = 10;

                boolean printFlushProgress = false;

                // Print flush progress

                // 如果距上次刷盘时间超过 flushPhysicQueueThoroughInterval ，则本次刷盘忽略 flushPhysicQueueLeastPages 参数，也就是即使写入的数量不足 flushPhysicQueueLeastPages，也执行刷盘操作。
                // 即：每 flushPhysicQueueThoroughInterval 周期执行一次 flush ，但不是每次循环到都能满足 flushCommitLogLeastPages 大小，
                // 因此，需要一定周期进行一次强制 flush 。当然，不能每次循环都去执行强制 flush，这样性能较差。
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {

                    // 根据 flushCommitLogTimed 参数，可以选择每次循环是固定周期还是等待唤醒。
                    // 默认配置是后者，所以，每次写入消息完成，会去调用 commitLogService.wakeup()
                    this.waitForRunning(interval);

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }


                    long begin = System.currentTimeMillis();
                    // todo 找到并调用 MappedFile 进行 flush
                    ScheduleLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }

                } catch (Throwable e) {
                    ScheduleLogManager.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // 执行到这里说明 Broker 关闭，强制 flush，避免有未刷盘的数据。
            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER; i++) {
                ScheduleLog.this.mappedFileQueue.flush(0);
                ScheduleLogManager.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            ScheduleLogManager.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /*------------------ 同步刷盘 -------------------*/

    /**
     * 同步刷盘请求对象
     */
    public static class GroupCommitRequest {
        /**
         * 提交的刷盘点(就是预计刷到哪里)，和怎么刷盘没有关系。主要用来判断，一次刷盘是否能完成本次刷盘任务。
         * todo 即：可能分布在两个 MappedFile(写第N个消息时，MappedFile 已满，创建了一个新的)，所以需要有循环2次。
         * <p>
         * 额外说明：同步刷盘和异步刷盘的区别：
         * 1 同步刷盘，调用方会提交一个同步刷盘请求，请求中指定了预计的刷盘点，要求立即刷盘，然后等待；
         * 2 异步刷盘，调用方尝试唤醒刷盘的线程就返回了，由刷盘线程根据刷盘页和刷盘周期决定什么时候真正刷盘。
         * 两者差不多的，区别在于同步刷盘是立即刷盘，异步刷盘可能不是立即刷盘而是需求达到一定的条件。
         */
        private final long nextOffset;

        /**
         * 刷盘 CompletableFuture
         */
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();


        private final long startTimestamp = System.currentTimeMillis();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }

        /**
         * 通知等待刷盘的线程任务完成（如有有等待的话）
         *
         * @param putMessageStatus
         */
        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * 消息追加成功时，同步刷盘时使用。
     */
    public class GroupCommitService extends FlushCommitLogService {
        // 写队列，用于存放刷盘任务
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();

        // 读队列，用于线程读取任务
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * 添加刷盘任务到写队列，然后唤醒执行任务
         * 说明：
         * 方法设置了 sync 的原因：this.requestsWrite 会和 this.requestsRead 不断交换，无法保证稳定的同步。
         *
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }

            // 有任务就创造立即刷盘条件，即唤醒可能阻塞等待的 GroupCommitService
            /*
               public void wakeup() {
                      if (hasNotified.compareAndSet(false, true)) {
                           waitPoint.countDown(); // notify
                          }
                  }
             */
            this.wakeup();
        }

        /**
         * 切换读写队列
         * todo 这是一个亮点设计：避免任务提交与任务执行的锁冲突。每次同步刷盘线程进行刷盘前都会将写队列切到成读队列，这样写队列可以继续接收同步刷盘请求，而刷盘线程直接从读队列读取任务然后进行刷盘。
         */
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        /**
         * 刷盘
         */
        private void doCommit() {

            // 上锁 requestsRead
            synchronized (this.requestsRead) {

                // 循环队列，进行 flush
                if (!this.requestsRead.isEmpty()) {

                    // 遍历刷盘请求
                    for (GroupCommitRequest req : this.requestsRead) {

                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        // todo 是否刷盘成功，比对 刷盘点和提交的预期刷盘点，看是否需要刷盘
                        boolean flushOK = ScheduleLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                        // todo 考虑到有可能每次循环写入的消息，可能分布在两个 MappedFile(写第N个消息时，MappedFile 已满，创建了一个新的)，所以需要有循环2次。
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            // 1 执行刷盘操作，这里刷盘页设置为 0 ，表示立即刷盘
                            ScheduleLog.this.mappedFileQueue.flush(0);

                            // 是否满足需要 flush 条件，即请求的 offset 超过 fLush 的 offset
                            flushOK = ScheduleLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        }

                        // 2 todo 通知等待同步刷盘的线程刷盘完成，避免其一直等待。即标记 flushOKFuture 完成
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = ScheduleLog.this.mappedFileQueue.getStoreTimestamp();

                    // checkpoint 更新 CommitLog 刷盘时间
                    if (storeTimestamp > 0) {
                        ScheduleLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    // 清理读队列
                    // 每次刷盘时，都会先将写队列切成读队列
                    this.requestsRead.clear();

                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    // 直接刷盘。此处是由于发送的消息的 isWaitStoreMsgOK 未设置成 TRUE ，导致未走批量提交
                    ScheduleLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        /**
         * 线程一直处理同步刷盘，每处理一个循环后等待 10 毫秒，一旦新任务到达，立即唤醒执行任务
         */
        @Override
        public void run() {
            ScheduleLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {

                    // 等待 10 毫秒
                    this.waitForRunning(10);

                    // 执行 doCommit() 方法
                    this.doCommit();
                } catch (Exception e) {
                    ScheduleLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                ScheduleLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            ScheduleLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return ScheduleLog.GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }


    /**
     * 销毁 ScheduleLog
     */
    public void destroyScheduleLog() {
        // 时间轮是一个非常耗费资源的结构，所以一个 jvm 中的实例数目不能太高
        this.hashedWheelTimer.stop();
        this.hashedWheelTimer = null; // help GC
        try {
            File file = new File(this.getScheduleDir());
            if (file.exists() && file.delete()) {
                System.out.println(this.getScheduleDir() + " 文件夹被清理！ ");
            }
        } catch (Exception ex) {
            log.warn("delete file dir failed，dir = {}", this.getScheduleDir());
        }
    }

    /**
     * 加载磁盘文件到内存映射文件
     *
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 恢复 ScheduleLog
     */
    public void recoverNormally() {
        // 是否验证 CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();

        // 获取 CommitLog 的内存文件列表
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            // 从哪个内存文件开始恢复，文件数 >= 3 时则从倒数第 3 个文件开始
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }
            MappedFile mappedFile = mappedFiles.get(index);

            // 内存映射文件对应的 ByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // 该文件的起始物理偏移量，默认从 CommitLog 中存放的第一个条目开始。
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;

            while (true) {

                // 使用 byteBuffer 逐条消息构建请求转发对象，直到出现异常或者当前文件读取完（换下一个文件）
                // 即使用能否构建消息转发对象作为有效性的标准
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                // 消息大小
                int size = dispatchRequest.getMsgSize();

                // Normal data
                // 数据正常
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                // 来到文件末尾，切换到下一个文件
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                // 文件读取错误
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            // processOffset 代表了当前 CommitLog 有效偏移量
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);

            // 删除有效偏移量后的文件 ，保留文件中有效数据（本质上：更新 MappedFile 的写指针、提交指针、刷盘指针）
            // 即 截断无效的 ScheduleLog 文件，只保留到 processOffset 位置的有效文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

        } else {
            // ScheduleLog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
        }
    }


    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 构建转发请求对象 DispatchRequest
     * <p>
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {

            /*---------- 根据消息在 CommitLog 中的存储格式，进行读取 --------------*/

            // 1 TOTAL SIZE 当前消息条目大小
            int totalSize = byteBuffer.getInt();
            // 2 MAGIC CODE 魔数
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            // 消息体的 crc 校验码
            int bodyCRC = byteBuffer.getInt();
            // todo 消息消费队列ID
            int queueId = byteBuffer.getInt();
            // FLAG FLAG 消息标记，RocketMQ 对其不做处理，供应用程序使用
            int flag = byteBuffer.getInt();
            // todo 消息队列逻辑偏移量
            long queueOffset = byteBuffer.getLong();
            // todo 消息在 CommitLog 文件中的物理偏移量
            long physicOffset = byteBuffer.getLong();
            // 消息系统标记，例如是否压缩、是否是事务消息等
            int sysFlag = byteBuffer.getInt();
            // 消息生产者调用消息发送 API 的时间戳
            long bornTimeStamp = byteBuffer.getLong();
            // 消息发送者 IP、端口号
            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            // todo 消息存储时间戳
            long storeTimestamp = byteBuffer.getLong();

            // Broker 服务器 IP + 端口号
            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            // todo 消息重试次数
            int reconsumeTimes = byteBuffer.getInt();
            // Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            // 消息体长度
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            // Topic 主题存储长度
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);
            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;


            // todo 处理附加属性，很重要
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);

                // 解析出附加属性
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                // 取出 KEYS
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                // 取出 UNIQ_KEY，消息的 msgId
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                // 取出 TAGS
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                // todo 计算 tag 的 hashCode
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }
            }

            // 计算消息长度
            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            // 校验消息长度
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            /**
             * 构建转发请求对象 DispatchRequest
             */
            return new DispatchRequest(
                    topic, // 消息主题
                    queueId, // 队列id
                    physicOffset, // 消息在 CommitLog 文件中的物理偏移量
                    totalSize, // 消息大小
                    tagsCode, // 如果是延时消息，就是延时时间
                    storeTimestamp, // 消息存储时间
                    queueOffset, // 消息逻辑队列偏移量
                    keys,
                    uniqKey, // msgId
                    sysFlag, // 消息系统标记
                    preparedTransactionOffset,
                    propertiesMap // 消息的附加属性
            );

        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        /**
         * RocketMQ 消息存储格式如下：
         */
        final int msgLen =
                4 //TOTALSIZE 消息条目总长度，4字节。注意：CommitLog 条目是不定长的，每个条目的长度存储在前 4 个字节
                        + 4 //MAGICCODE 魔数，4 字节
                        + 4 //BODYCRC 消息体的 crc 校验码，4字节
                        + 4 //QUEUEID 消息消费队列ID，4字节
                        + 4 //FLAG 消息标记，RocketMQ 对其不做处理，供应用程序使用，默认4字节
                        + 8 //QUEUEOFFSET 消息在 ConsumeQueue 文件中的逻辑偏移量，8字节
                        + 8 //PHYSICALOFFSET 消息在 CommitLog 文件中的物理偏移量，8字节
                        + 4 //SYSFLAG 消息系统标记，例如是否压缩、是否是事务消息等
                        + 8 //BORNTIMESTAMP 消息生产者调用消息发送 API 的时间戳，8字节
                        + bornhostLength //BORNHOST 消息发送者 IP、端口号，8字节
                        + 8 //STORETIMESTAMP 消息存储时间戳，8字节
                        + storehostAddressLength //STOREHOSTADDRESS Broker 服务器 IP + 端口号，8字节
                        + 4 //RECONSUMETIMES 消息重试次数，4字节
                        + 8 //Prepared Transaction Offset  事务消息的物理偏移量 8字节
                        + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY 消息体长度 4字节
                        + 1 + topicLength //TOPIC 主题存储长度，1字节。从这里也可以看出，主题名称不能超过 255 个字符
                        + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength 消息属性长度，2字节。从这也可以看出，消息属性长度不能超过 65536
                        + 0;
        return msgLen;
    }


    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long rollNextFile(final long offset) {
        // 获取每个 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // （offset + 文件大小 ） -> 跳到下一个文件
        // 减去多余的 offset ，就可以得到起始偏移量
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    /**
     * todo 读取从物理偏移量到 size 大小的数据，如：size = 4，读取的就是消息的长度（因为 CommitLog 和 ConsumeQueue 存储不一样，前者是不定长的，后者是定长的 20字节）
     * 说明：
     * 主要根据物理偏移量，找到所在的commitlog文件，commitlog文件封装成MappedFile(内存映射文件)，然后直接从偏移量开始，读取指定的字节（消息的长度），
     * 要是事先不知道消息的长度，只知道offset呢？其实也简单，先找到MapFile,然后从offset处先读取4个字节，就能获取该消息的总长度。
     *
     * @param offset 物理偏移量
     * @param size   消息大小
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 1 获取 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // 2 根据偏移量 offset ，找到偏移量所在的 Commitlog 文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);

        // 3 找到对应的 MappedFile 后，开始根据偏移量和消息长度查找消息
        if (mappedFile != null) {

            // 根据 offset 计算在单个 MappedFile 中的偏移量
            int pos = (int) (offset % mappedFileSize);

            // 从偏移量读取 size 长度的内容并返回
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 删除过期的 ScheduleLog
     *
     * @param deleteFilesInterval
     * @param intervalForcibly
     * @param cleanImmediately
     * @return
     */
    public int deleteExpiredFile(
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean forceCleanAll,
            final boolean cleanImmediately) {
        return this.mappedFileQueue.deleteExpiredFileByTime(deleteFilesInterval, intervalForcibly, forceCleanAll, cleanImmediately);
    }


    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    /**
     * 销毁 ScheduleLog
     */
    public void destroy() {
        this.maxPhysicOffset = -1;

        // 删除文件组
        this.mappedFileQueue.destroy();

        // 关闭刷盘线程任务
        flushCommitLogService.shutdown();

        // 销毁其它资源
        this.destroyScheduleLog();

        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public String getScheduleDir() {
        return scheduleDir;
    }

    public HashedWheelTimer getHashedWheelTimer() {
        return hashedWheelTimer;
    }

    public void setHashedWheelTimer(HashedWheelTimer hashedWheelTimer) {
        this.hashedWheelTimer = hashedWheelTimer;
    }

    public FlushCommitLogService getFlushCommitLogService() {
        return flushCommitLogService;
    }
}
