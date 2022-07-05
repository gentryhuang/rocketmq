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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * RocketMQ 存储核心类，Broker 持有。包含了很多对存储文件进行操作的 API，其他模块对消息实体的操作都是通过该类进行的。
 * 说明：
 * 1 RocketMQ 存储用的是本地文件存储系统，效率高也可靠
 * 2 主要涉及到三种类型的文件：CommitLog、ConsumeQueue、IndexFile
 * - CommitLog: 所有主题的消息都存在 CommitLog 中，单个 CommitLog 默认为 1G，并且文件名以起始偏移量命令，固定 20 位，不足前面补 0 。
 * 比如 00000000000000000000 代表了第一个文件，第二个文件名就是 00000000001073741824，表明起始偏移量为 1073741824，
 * 以这样的方式命名用偏移量就能找到对应的文件。所有消息都是顺序写入的，超过文件大小则开启下一个文件。
 * - ConsumeQueue:
 * 1）消息消费队列，可以认为是 CommitLog 中消息的索引，因为 CommitLog 是糅合了所有主题的消息，所以通过索引才能更加高效的查找消息。
 * 2）ConsumeQueue 存储的条目是固定大小，只会存储 8 字节的 CommitLog 物理偏移量，4 字节的消息长度和 8 字节 Tag 的哈希值，固定 20 字节。
 * 3）在实际存储中，ConsumeQueue 对应的是一个Topic 下的某个 Queue，每个文件约 5.72M，由 30w 条数据组成。
 * 4）消费者是先从 ConsumeQueue 来得到消息真实的物理地址，然后再去 CommitLog 获取消息
 * - IndexFile:
 * 1) 索引文件，是额外提供查找消息的手段，不影响主流程
 * 2) 通过消息ID、 Key 或者时间区间来查询对应的消息，文件名以创建时间戳命名，固定的单个 IndexFile 文件大小约为400M，一个 IndexFile 存储 2000W个索引。
 * 3 消息到了先存储到 CommitLog，然后会有一个 ReputMessageService 线程接近实时地将消息转发给消息消费队列文件与索引文件，也就是说是异步生成的。
 * 4 CommitLog 采用混合型存储，也就是所有 Topic 都存在一起，顺序追加写入，文件名用起始偏移量命名。
 * 5 消息先写入 CommitLog 再通过后台线程分发到 ConsumerQueue 和 IndexFile 中。
 * 6 消费者先读取 ConsumerQueue 得到真正消息的物理地址，然后访问 CommitLog 得到真正的消息。
 * 7 利用了 mmap 机制减少一次拷贝，利用文件预分配和文件预热提高性能
 * 8 提供同步和异步刷盘，根据场景选择合适的机制。
 * <p>
 * 9 文件清除机制 - {@link org.apache.rocketmq.store.DefaultMessageStore#addScheduleTask()} 使用的是定时任务的方式，在 Broker 启动时会触发 {@link org.apache.rocketmq.store.DefaultMessageStore#start()}
 * - 9.1 RocketMQ 操作 CommitLog 和 ConsumeQueue 文件，都是基于内存映射文件。Broker 在启动时会加载 CommitLog、ConsumeQueue 对应的文件夹，读取物理文件到内存以创建对应的内存映射文件，
 * 为了避免内存与磁盘的浪费，不可能将消息永久存储在消息服务器上，所以需要一种机制来删除已过期的文件。
 * - 9.2 RocketMQ 顺序写 CommitLog 、ConsumeQueue 文件，所有写操作全部落在最后一个 CommitLog 或 ConsumeQueue 文件上，之前的文件在下一个文件创建后，将不会再被更新。
 * - 9.3 RocketMQ 清除过期文件的方法是：如果非当前写文件在一定时间间隔内没有再次被更新，则认为是过期文件，可以被删除，RocketMQ不会管这个这个文件上的消息是否被全部消费。默认每个文件的过期时间为72小时。
 * - 9.4 消息是被顺序存储在 commitlog 文件的，且消息大小不定长，所以消息的清理是不可能以消息为单位进 行清理的，而是以commitlog 文件为单位进行清理的。否则会急剧下降清理效率，并实现逻辑复杂。
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息存储配置属性
     * 存储相关的配置，如存储路径、commitLog 文件大小、刷盘频次等
     */
    private final MessageStoreConfig messageStoreConfig;


    /**
     * CommitLog 文件的存储实现类
     * <p>
     * 一个即可，因为所有主题消息都存在 CommitLog 中
     */
    private final CommitLog commitLog;

    /**
     * topic 的队列文件缓存表，按消息主题分组
     * <p>
     * 针对某个 Topic，对于每个队列都有对应的 ConsumeQueue
     *
     * @see DefaultMessageStore#findConsumeQueue(java.lang.String, int)
     * @see DefaultMessageStore#loadConsumeQueue()
     */
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    /**
     * ConsumeQueue 文件刷盘线程任务
     */
    private final FlushConsumeQueueService flushConsumeQueueService;

    /**
     * consumeQueue 过期文件删除任务
     */
    private final CleanConsumeQueueService cleanConsumeQueueService;

    /**
     * CommitLog 过期文件清除线程任务
     */
    private final CleanCommitLogService cleanCommitLogService;


    /**
     * MappedFile 分配线程任务，RocketMQ 使用内存映射处理 CommitLog、ConsumeQueue文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * CommitLog 消息分发线程任务，根据 CommitLog 文件构建 ConsumeQueue、Index 文件
     * todo 重要，理解 Commitlog 和 Consumequeue 的关系
     */
    private final ReputMessageService reputMessageService;

    /**
     * 存储高可用机制
     * 主从同步实现服务
     */
    private final HAService haService;

    /**
     * 延时任务调度器，执行延时任务
     */
    private final ScheduleMessageService scheduleMessageService;

    /**
     * Index 文件实现类
     */
    private final IndexService indexService;

    /**
     * 存储统计服务
     */
    private final StoreStatsService storeStatsService;

    /**
     * 消息堆外内存池
     * ByteBuffer 池
     */
    private final TransientStorePool transientStorePool;

    /**
     * 存储服务状态
     */
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));

    /**
     * Broker 统计服务
     */
    private final BrokerStatsManager brokerStatsManager;

    /**
     * todo 在消息拉取长轮询模式下的消息达到监听器
     */
    private final MessageArrivingListener messageArrivingListener;
    /**
     * Broker 配置属性
     */
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    /**
     * 文件刷盘检测点
     */
    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    /**
     * todo CommitLog 文件转发请求
     * <p>
     * 转发 commitLog 日志，主要是从 commitlog 转发到 consumeQueue、index 文件
     * consumeQueue -> CommitLogDispatcherBuildConsumeQueue
     * index -> CommitlogDispatcherBuildIndex
     */
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    /**
     * 存储核心类，集中存储相关的核心类
     *
     * @param messageStoreConfig
     * @param brokerStatsManager
     * @param messageArrivingListener
     * @param brokerConfig
     * @throws IOException
     * @see org.apache.rocketmq.broker.BrokerController#initialize()
     */
    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);

        // 初始化一个 CommitLog
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);

        // 创建主从复制的 HAService 对象
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }

        // 创建重放消息服务
        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        // 启动 MappedFile 预分配任务
        this.allocateMappedFileService.start();

        this.indexService.start();

        // 创建 CommitLog 派发器，目前有两个实现：一个用于建构 ConsumeQueue ，另一个构建 IndexFile
        // todo 改造延时消息的话，可以新定义一个 Dispatcher ，专门派发延时消息类型的派发请求
        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    /**
     * 根据 CommitLog 有效的物理偏移量，清除无效的 ConsumeQueue 数据
     *
     * @param phyOffset CommitLog 最大有效的物理偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 在 RocketMQ 启动过程中，是如何根据 commitlog 重构 consumequeue ，index 的，因为毕竟 commitlog 文件中的消息与 consumequeue 中的文件内容并不能确保是一致的。
     * <p>
     * 过程如下：
     * 1 加载相关文件到内存（内存映射文件）
     * - 包含 CommitLog 文件、
     * - ConsumeQueue 文件、
     * - 存储检测点（CheckPoint）文件、
     * - 索引 IndexFile 文件
     * 2 执行文件恢复
     * 3 恢复顺序：
     * - 先恢复 ConsumeQueue 文件，把不符合的 ConsumeQueue 文件删除，一个 ConsumeQueue 条目正确的标准（commitlog偏移量 >0 size > 0）[从倒数第三个文件开始恢复]。
     * - 如果 abort 文件存在，此时找到第一个正常的 commitlog 文件，然后对该文件重新进行转发，依次更新 consumeque,index文件 （非正常逻辑）；正常逻辑和恢复 consumeque 文件恢复过程类似。
     *
     * @throws IOException
     * @see org.apache.rocketmq.broker.BrokerController#initialize()
     */
    public boolean load() {
        boolean result = true;

        try {
            // 1 判断 user.home/store/abort 文件是否存在
            // Broker 在启动过时创建，在退出时通过注册 JVM 钩子函数删除 abrot 文件。也就是如果该文件存在，说明不是正常的关闭。
            // todo 在 Broker 异常退出时，CommitLog 与 ConsumeQueue 数据有可能不一致，需要进行恢复
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // 2 需要处理延迟消息，包括 维护 delayLevelTable 映射表 和加载文件
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // 3 加载 CommitLog 文件
            result = result && this.commitLog.load();

            // 4 加载 ConsumeQueue 文件
            result = result && this.loadConsumeQueue();

            // 以上的文件都加载成功后，加载并存储 checkpoint 文件
            if (result) {
                // 5 文件存储检测点封装对象
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // 6 索引文件加载
                this.indexService.load(lastExitOK);

                // 7 根据 Broker 是否正常停止，选择不同策略进行文件恢复
                // todo 对前面加载的 ConsumeQueue 和 CommitLog 进行恢复
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * todo 非常重要
     * 存储服务启动会启动多个后台线程
     *
     * @throws Exception
     * @see org.apache.rocketmq.broker.BrokerController#start()
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);


        /*-----------todo  获取 CommitLog 最小物理偏移量，用于指定从哪个物理偏移量开始转发消息给 ConsumeQueue 和 Index 文件 -------------*/
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             */
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();

            // 遍历队列，获取最大偏移量，因为重放消息，不能重复。先看 ConsumeQueue 已经重放消息到哪了，如果写入的位置大于 CommitLog 最小物理偏移量，那么就以写入的位置开始尝试重放消息
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                // 消息的索引是分散到不同的队列中的，因此要找到最大的
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }

            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }

            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }

            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                    maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());




            /*--------- todo  重放消息开始位置 & 启动重放消息的 CommitLog 的任务构建 ConsumeQueue 和 IndexFile & 启动 CommitLog 和 ConsumeQueue 刷盘任务 -----------*/
            /*
               todo  CommitLog 和 ConsumeQueue 是如何保证一致性的？
               - 在 Broker 启动时，会对 CommitLog 和 ConsumeQueue 进行恢复，把无效的 CommitLog 和 ConsumeQueue 数据删除掉，其中 ConsumeQueue 以 CommitLog 为准，保证消息关系正确、一致
                  @see org.apache.rocketmq.store.DefaultMessageStore.recover
               - RocketMQ 在运行期间，可以认为消息重放不会有问题的，在重放 ConsumeQueue 会重试 30 次，即使这期间失败的话，会将对象设置为不可写。
               - ReputMessageService 任务会马不停蹄地做消息重放
               总结来说，RocketMQ 在 Broker 启动时就把一致性问题解决了，运行期间不会发生问题。即使断电异常，下次启动又会校验、恢复。
             */

            // todo 设置从 CommitLog 哪个物理偏移量开始转发消息给 ConsumeQueue 和 Index 文件
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);

            // todo 启动 Commitlog 转发到 Consumequeue、Index文件 的任务，该任务基本不停歇地执行
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                // 等待剩余需要重放消息字节数完成
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }

            // 主要用于恢复 CommitLog 中维护的 HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable
            this.recoverTopicQueueTable();
        }

        // todo 根据情况启动：1）高可用服务 2）启动延时消息处理任务
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {

            // 启动 HA
            this.haService.start();

            // 启动处理延迟消息
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        // todo 启动 ConsumeQueue 刷盘任务
        this.flushConsumeQueueService.start();


        // todo commitLog 刷盘任务
        this.commitLog.start();


        this.storeStatsService.start();

        this.createTempFile();

        // todo  开启系列定时任务，其中包含用来清理过期文件
        this.addScheduleTask();

        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.diskCheckScheduledExecutorService.shutdown();
            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    /**
     * 检查消息
     *
     * @param msg
     * @return
     */
    private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
        // 消息主题长度检查
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        // 消息属性长度检查
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + messageExtBatch.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        return PutMessageStatus.PUT_OK;
    }

    /**
     * 检查存储服务状态
     *
     * @return
     */
    private PutMessageStatus checkStoreStatus() {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("broke role is slave, so putMessage is forbidden");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        // 当前是否可写
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("the message store is not writable. It may be caused by one of the following reasons: " +
                        "the broker's disk is full, write to logic queue error, write to index file error, etc");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        } else {
            this.printTimes.set(0);
        }


        // 判断 OS 是否忙碌
        if (this.isOSPageCacheBusy()) {
            return PutMessageStatus.OS_PAGECACHE_BUSY;
        }
        return PutMessageStatus.PUT_OK;
    }

    /**
     * 异步存储消息
     *
     * @param msg MessageInstance to store
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        // 当前存储服务是否可以写
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        // 消息检查
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }


        // 异步消息存储
        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        // todo putResultFuture 执行完会回调该方法，但是执行线程不会等待，它会直接返回
        putResultFuture.thenAccept((result) -> {
            // 统计消耗时间
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return putResultFuture;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> resultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        resultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
            }

            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return resultFuture;
    }

    /**
     * 消息存储
     * 说明：存储消息封装，最终存储需要 CommitLog 实现。
     *
     * @param msg Message instance to store
     * @return
     */
    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 检查存储服务状态
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        // 对消息进行检查
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        // todo 消息写入计时开始
        long beginTime = this.getSystemClock().now();

        // todo 重点关注
        // 将消息写入 CommitLog 文件，具体实现类 CommitLog
        PutMessageResult result = this.commitLog.putMessage(msg);

        // todo 消息写入计时结束，计算本次消息写入耗时
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
        }

        // 记录相关统计信息
        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        // 记录写 commitlog 失败次数
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
        }

        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    /**
     * 判断操作系统PageCache是否繁忙，如果忙，则返回true。
     *
     * @return
     */
    @Override
    public boolean isOSPageCacheBusy() {
        // 消息开始写入 Commitlog 文件时加锁的时间
        long begin = this.getCommitLog().getBeginTimeInLock();

        // 一次消息追加过程中截止到当前持有锁的总时长，完成后重置为 0
        // 即往内存映射文件或pageCache追加一条消息直到现在所耗时间
        long diff = this.systemClock.now() - begin;

        // 如果一次消息追加过程的时间超过了Broker配置文件osPageCacheBusyTimeOutMills ，默认 1000 即 1s，则认为pageCache繁忙，
        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 获取消息
     *
     * @param group         消费组名称
     * @param topic         消息主题
     * @param queueId       消息队列id
     * @param offset        拉取的消息队列逻辑偏移量
     * @param maxMsgNums    一次拉取消息条数，默认为 32
     * @param messageFilter 消息过滤器 - 如根据 tag 过滤「注意什么时候创建的」
     * @return
     */
    public GetMessageResult getMessage(final String group,
                                       final String topic,
                                       final int queueId,
                                       final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }
        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        // 拉取消息的队列偏移量
        long nextBeginOffset = offset;
        // 当前消息队列最小偏移量
        long minOffset = 0;
        // 当前消息队列最大偏移量
        long maxOffset = 0;

        // 拉取消息的结果
        GetMessageResult getResult = new GetMessageResult();

        // 3 获取 CommitLog 文件中的最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        // 4 根据 topic、queueId 获取消息队列（ConsumeQueue）
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            // 选中的消息队列，最小编号，todo 最小逻辑偏移量
            minOffset = consumeQueue.getMinOffsetInQueue();
            //  选中的消息队列，最大编号，todo 最大逻辑偏移量
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            // 6 todo 根据需要拉取消息的偏移量 与 队列最小，最大偏移量进行对比，并修正拉取消息偏移量

            // 队列中没有消息
            // 1）如果是主节点，或者是从节点但开启了offsetCheckSlave的话，下次从头开始拉取，即 0。
            // 2）如果是从节点，并不开启 offsetCheckSlave,则使用原先的 offset,因为考虑到主从同步延迟的因素，导致从节点consumequeue并没有同步到数据。
            // offsetCheckInSlave设置为false保险点，当然默认该值为false。返回状态码： NO_MESSAGE_IN_QUEUE。
            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);

                // 表示要拉取的偏移量小于队列最小的偏移量
                // 如果是主节点，或开启了offsetCheckSlave的话，设置下一次拉取的偏移量为minOffset
                // 如果是从节点，并且没有开启offsetCheckSlave,则保持原先的offset,这样的处理应该不合适，因为总是无法满足这个要求 ，返回status : OFFSET_TOO_SMALL,估计会在消息消费拉取端重新从消费进度处获取偏移量，重新拉取。
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);

                // 待拉取偏移量为队列最大偏移量，表示超出一个，返回状态：OFFSET_OVERFLOW_ONE，
                // 下次 拉取偏移量依然为 offset 保持不变。
                // todo offset  <  (mappedFile.getFileFromOffset() + mappedFile.getReadPosition())/20，因为写指针停留的位置是下一次开始写入数据的位置
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);

                // 偏移量越界，返回状态：OFFSET_OVERFLOW_BADLY
                // 如果为从节点并未开启 offsetCheckSlave,则使用原偏移量，这个是正常的，等待消息到达从服务器。
                // 如果是主节点：表示offset是一个非法的偏移量，如果minOffset=0,则设置下一个拉取偏移量为0,否则设置为最大。
                // todo 疑问：设置为0，重新拉取，有可能消息重复吧？？？，设置为最大可能消息会丢失？什么时候会offset > maxOffset(在主节点）拉取完消息，进行第二次拉取时，重点看一下这些状态下，应该还有第二次修正消息的处理。
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }

                // offset 大于minOffset 并小于 maxOffset ，正常情况。
            } else {

                // 7 从 consuequeue 中根据 offset 这个逻辑偏移量获取消息索引信息，信息量是从 offset（会转为对应的物理偏移量） 到当前 ConsumeQueue 中最大可读消息
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);

                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        // 8 初始化基本变量。

                        // 下一个开始 offset
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        // 根据传入的拉取最大消息数，计算要过滤的消息索引长度。即最大过滤消息字节数。
                        // todo 注意，为什么不直接 maxFilterMessageCount = maxMsgNums * 20 ，因为拉取到的消息可能不满足过滤条件，导致拉取的消息小于 maxMsgNums，
                        //  那这里一定会返回maxMsgNums条消息吗？不一定，这里是尽量返回这么多条消息。
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        // 对读取的消息索引进行逐条遍历直到达到预期的消息数，从遍历条件：i += ConsumeQueue.CQ_STORE_UNIT_SIZE 也可以看出
                        // 注意，即使没有够传入的拉取最大消息数，但是获取的消息索引遍历完了，那么不用管了，等待下次拉取请求。
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {

                            /*读取一个消息索引的 3 个属性：在 CommitLog 中的物理偏移量、消息长度、tag 的哈希值*/
                            // 消息物理位置offset
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            //  消息长度
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            // todo 消息 tagsCode（作为过滤条件）
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            // 11 当前拉取的物理偏移量，即拉取到的最大offset
                            // 按照消息顺序拉取的基本原则，可以基本预测下次开始拉取的物理偏移量将大于该值，并且就在其附近。
                            maxPhyOffsetPulling = offsetPy;

                            // 12 如果拉取到的消息偏移量小于下一个要拉取的物理偏移量的话，直接跳过该条消息
                            // 即当 offsetPy 小于 nextPhyFileStartOffset 时，意味着对应的 Message 已经移除，所以直接 continue 跳过，直到可读取的Message
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 13 校验当前物理偏移量 offsetPy 对应的消息是否还在内存，即校验 commitLog 是否需要硬盘，因为一般消息过大无法全部放在内存
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            // 14 本次是否已拉取到足够的消息，如果足够则结束
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
                                break;
                            }

                            // todo 根据偏移量拉取消息索引后，根据 ConsunmeQueue 条目进行消息过滤。如果不匹配则直接跳过该条消息，继续拉取下一条消息。
                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            // 15 todo 执行消息过滤，如果符合过滤条件,则直接进行消息拉取，如果不符合过滤条件，则进入继续执行逻辑。如果最终符合条件，则将该消息添加到拉取结果中。
                            if (messageFilter != null
                                    // todo 根据 tag 哈希码匹配当前的消息索引
                                    && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {

                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                // 不符合过滤条件，不拉取消息，继续遍历下一个消息索引
                                continue;
                            }

                            /*
                               todo 注意： Tag 过滤不需要访问 CommitLog 数据，可以保证高效过滤；此外，即使存在 Tag 的 hash 冲突，也没关系，因为在 Consume 端还会进行一次过滤以修正，保证万无一失。
                             */

                            // 16 有了消息物理偏移量和消息大小，就可以从 CommitLog 文件中读取消息，根据偏移量与消息大小
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {

                                // 从commitLog无法读取到消息，说明该消息对应的文件（MappedFile）已经删除，计算下一个MappedFile的起始位置
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                // 17 如果该物理偏移量没有找到正确的消息，则说明已经到文件末尾了，下一次切换到下一个 commitlog 文件读取
                                // 返回下一个文件的起始物理偏移量
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            // 18 todo 从 CommitLog（全量消息）再次过滤，ConsumeQueue 中只能处理 TAG 模式的过滤，SQL92 这种模式无法过滤，
                            //   因为SQL92 需要依赖消息中的属性，故在这里再做一次过滤。如果消息符合条件，则加入到拉取结果中。
                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {

                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            // 19 将消息加入到拉取结果中
                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();


                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;

                            // 重置
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        // 20 统计剩余可拉取消息字节数
                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        // 21 todo 计算下次从当前消息队列拉取消息的逻辑偏移量
                        // 即消费几条就推进几个进度
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);


                        /*--------------------- 22 建议消息拉取从哪个 Broker 。-------*/

                        // 当前未被拉取到消费端的 CommitLog 中的消息长度
                        long diff = maxOffsetPy - maxPhyOffsetPulling;

                        // RocketMQ 消息常驻内存的大小：40% * (机器物理内存)；超过该大小，RocketMQ 会将旧的消息置换回磁盘。
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE // RocketMQ 所在服务器的总内存大小
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0)// RocketMQ 所能使用内存的最大比例，默认 40 。
                        );

                        /**
                         * todo 触发下次从从服务器拉取的条件：剩余可拉取的消息大小 > RocketMQ 消息常驻内存的大小。
                         * 即 diff > memory 表示可拉取的消息大小已经超出了 RocketMQ 常驻内存的大小，反映了主服务器繁忙，建议从从服务器拉取消息。
                         * todo 该属性在 {PullMessageProcessor#processRequest} 处使用
                         */
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        // 释放 bufferConsumeQueue 对 MappedFile 的指向。此处 MappedFile 是 ConsumeQueue 里的文件，不是 CommitLog 下的文件。
                        bufferConsumeQueue.release();
                    }

                    // 从指定的偏移量没有获取到对应的消息索引信息，那么计算 ConsumeQueue 从 offset 开始的下一个 MappedFile 对应的位置。
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;

                    // 下一次从哪个逻辑偏移量拉取消息
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }

            // 没有对应的消息队列
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            // 下一次从哪个逻辑偏移量拉取消息
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        // 记录统计信息：消耗时间、拉取到消息/未拉取到消息次数
        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // 22 设置返回结果：设置下一次拉取偏移量，然后返回拉取结果
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    /**
     * 根据偏移量从 CommitLog 中查找对应的消息，具体过程：
     * 1 根据偏移量读取对应消息的长度，因为 CommitLog 前 4 个字节存储的是消息的长度
     * 2 知道了消息的长度，就可以获取消息了
     *
     * @param commitLogOffset physical offset.
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE ，消息的长度
                int size = sbr.getByteBuffer().getInt();
                // 根据消息大小获取消息
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    /**
     * 获取 CommitLog 物理存储路径
     *
     * @return
     */
    private String getStorePathPhysic() {
        String storePathPhysic = "";
        // 如果启动 Dleger
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            storePathPhysic = ((DLedgerCommitLog) DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();

            // CommitLog 默认存储路径： /Users/huanglibao/store/commitlog
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    /**
     * 将消息 data 追加到 CommitLog 内存映射文件中
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @return
     */
    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);

        // 追加成功，则唤醒 ReputMessageService 实时将消息转发给消息消费队列与索引文件
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    /**
     * 检查 topic 下队列 queueId 的 consumeOffset 逻辑偏移量对应的消息是否还在内存中
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return
     */
    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        // 消息的最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 根据 topic 和 queueId 查找消息队列
     *
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        // 获取 topic 对应的所有消费队列
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);

        // 如果没有则创建一个 ConsumeQueue，并缓存起来
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        // 获取 queueId 对应的 消费队列
        ConsumeQueue logic = map.get(queueId);

        // 没有则创建一个并缓存起来
        if (null == logic) {
            // 创建消息队列，注意一个逻辑 队列包含多个物理文件
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                    this);

            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 下一个获取队列 offset 修正
     * 修正条件：主节点 或者 从节点开启校验offset开关
     *
     * @param oldOffset 老队列 offset
     * @param newOffset 新队列 offset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;

        // 主节点 或者 从节点开启校验offset开关
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * 校验当前物理偏移量 offsetPy 对应的消息是否还在内存。
     * 即：如果 maxOffsetPy-offsetPy > memory 的话，说明 offsetPy 这个偏移量的消息已经从内存中置换到磁盘中了。
     *
     * @param offsetPy    commitLog 物理偏移量
     * @param maxOffsetPy commitLog 最大物理偏移量
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        // 驻内存的消息大小：物理内存总大小 * 消息贮存内存的比例
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));

        // 是否超过驻内猝的大小
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 判断本次拉取任务是否完成
     *
     * @param sizePy       当前消息字节长度
     * @param maxMsgNums   一次拉取消息条数，默认为 32
     * @param bufferTotal  截止到当前，已拉取消息字节总长度，不包含当前消息
     * @param messageTotal 截止到当前，已拉取消息总条数
     * @param isInDisk     当前消息是否存在于磁盘中
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        // 达到预期的拉取消息条数
        if (maxMsgNums <= messageTotal) {
            return true;
        }

        // 消息在磁盘上
        if (isInDisk) {
            // 已拉取消息字节数 + 待拉取消息的长度，达到了磁盘消息的传输上限（默认 64K)
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            // 已拉取消息条数，达到了磁盘消息传输的数量上限（默认 8）
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }

            // 消息在内存中，逻辑同上
        } else {
            // 默认 256K
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            // 默认 32
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 添加系列定期任务
     */
    private void addScheduleTask() {

        /**
         * todo 周期性清理文件。默认每 10s 检查一次过期文件
         * 说明：
         * 1 RocketMQ 操作 CommitLog 、ConsumeQueue 文件，都是基于内存映射方法并在启动的时候，会加载 commitlog、consumequeue 目录下的所有文件.
         * 2 为了避免内存与磁盘的浪费，不可能将消息永久存储在消息服务器，所以需要一种机制来删除已过期的文件。
         * 3 RocketMQ 顺序写 CommitLog、ConsumeQueue 文件，所有写操作全部落在最后一个 CommitLog 或 ConsumeQueue 文件上，之前的文件在下一个文件创建后，将不会再被更新。
         * 4 RocketMQ清除过期文件的方法是：
         *   4.1 如果非当前写文件在一定时间间隔内没有再次被更新，则认为是过期文件，可以被删除。默认每个文件的过期时间为72小时。可以通过在Broker配置文件中设置fileReservedTime来改变过期时间，单位为小时。
         *   4.2 清理过期文件时，RocketMQ 不会管这个文件上的消息是否被全部消费
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // todo 定期执行过期文件清理工作，默认 10s 执行一次。有三种情况会执行清理，具体看流程。
                // 主要清理 CommitLog 、ConsumeQueue、Index  的过期文件。如何定义文件过期，两个文件的判断方式大同小异。
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
        this.diskCheckScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
            }
        }, 1000L, 10000L, TimeUnit.MILLISECONDS);
    }

    /**
     * todo 过期文件清理
     */
    private void cleanFilesPeriodically() {
        // 1 CommitLog 文件清理
        this.cleanCommitLogService.run();

        // 2 ConsumeQueue 文件清理 && Index 文件清理
        // todo 注意，是先清理 CommitLog 的，因为清理 ConsumeQueue 文件和 Index 文件要以 CommitLog 为准（以消息最小物理偏移量）
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    /**
     * 加载消息消费队列
     *
     * @return
     */
    private boolean loadConsumeQueue() {
        // 封装 $user.home/store/consumequeue 文件夹
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));

        // 获取 Store consumequeue 目录下所有子目录，加载进来
        File[] fileTopicList = dirLogic.listFiles();

        if (fileTopicList != null) {

            // 遍历 Topic 级别的目录
            for (File fileTopic : fileTopicList) {

                // 文件夹对应的名 - Topic
                String topic = fileTopic.getName();

                // Topic 文件夹下的文件列表 - queue 文件列表
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {

                    // 遍历 queue 文件列表
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            // 获取 queueId
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        // todo 还原当前 queueId 对应的 ConsumeQuueue ，这是一个逻辑文件，具体由 MappedFileQueue 管理多个映射文件 MappedFile
                        // 构建 ConsumeQueue ，主要初始化：topic、queueId、storePah、mappedFileSize
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                                this);

                        // todo 将文件中的 ConsumeQueue 加载到内存
                        this.putConsumeQueue(topic, queueId, logic);

                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 文件恢复
     *
     * @param lastExitOK Broker 是否正常关闭
     */
    private void recover(final boolean lastExitOK) {
        // 1 恢复消息队列。返回的结果是 ConsumeQueue 中写入的消息最大物理偏移量
        // 即移除非法的 offset
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        // 2 恢复 CommitLog

        // 2.1 如果是正常退出，则按照正常修复
        if (lastExitOK) {
            // 即移除非法的 offset，并且进一步移除 ConsumeQueue 的非法 offset
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);

            // 2.2 如果异常退出，则按照异常修复逻辑
        } else {
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        // 3 修复主题队列
        // todo 恢复 ConsumeQueue 和 CommitLog 文件后，在 CommitLog 实例中保存每个消息消费队列当前的逻辑偏移量，这个用于处理消息发送和消息重放
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    /**
     * 更新存储服务内存中的 consumeQueueTable
     *
     * @param topic
     * @param queueId
     * @param consumeQueue
     */
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 恢复并返回 ConsumeQueue 中写入的消息最大物理偏移量（针对 CommitLog）
     *
     * @return
     */
    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // 恢复 ConsumeQueue 文件，只保留有效的
                logic.recover();

                // 获取记录 CommitLog 的最大物理偏移量
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }


    /**
     * 恢复主题队列
     */
    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);

        // 获取 CommitLog 最小的物理偏移量
        long minPhyOffset = this.commitLog.getMinOffset();

        // 遍历 Topic 下消息队列 ConsumeQueue
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {

            // 根据
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                // 更新 topic-queueid 的逻辑偏移量
                table.put(key, logic.getMaxOffsetInQueue());

                // logic 中最小偏移量
                logic.correctMinOffset(minPhyOffset);
            }
        }

        // 更新 CommitLog 中 Topic-queueid 对应逻辑偏移量
        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 执行调度请求
     * 1 非事务消息 或 事务提交消息 建立 消息位置信息 到 ConsumeQueue
     * 2 建立 索引信息 到 IndexFile
     *
     * @param req
     */
    public void doDispatch(DispatchRequest req) {
        // 转发 commitLog 日志，主要是从 commitlog 转发到 consumeQueue、index 文件
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }


    /**
     * 建立 消息位置信息到 ConsumeQueue
     *
     * @param dispatchRequest
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        // 1 根据消息 topic 和 queueId ，获取对应的 ConsumeQueue 文件
        // 找不到对应 ConsumeQueue 会自动创建一个
        // todo 因为每一个消息主题下每一个消息队列对应一个文件夹
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());

        // 2 同步 commitlog 内容，即建立消息位置信息到 当前获取的 ConsumeQueue
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }


    /**
     * 获取消息堆外内存池是否还有堆外内存可使用
     *
     * @return
     */
    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * consumequeue 对应的派发器，用于和 commitlog 文件同步
     * 特别说明：
     * 对于事务消息，只有提交 COMMIT 后才会生成 ConsumeQueue
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            // 非事务消息 或 事务提交消息, 建立 消息位置信息 到 ConsumeQueue
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: // 非事务消息
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: // 事务消息 COMMIT
                    // 建立消息位置到 ConsumeQueue
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE: // 事务消息PREPARED
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: // 事务消息ROLLBACK
                    break;
            }
        }
    }

    /**
     * CommitLog 派发器-根据 CommitLog 构建 Index
     * 特别说明：
     * 对于构建索引信息到 IndexFile，无需区分消息类型
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * commitLog 过期文件删除任务
     */
    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;

        /*-------------------------------- RocketMQ 提供了两个与磁盘空间使用率相关的系统级参数，如下：-------------------*/

        /**
         * 通过系统参数设置，默认值为 0.90。如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时会拒绝写入新消息，并且立即启动文件删除操作。
         */
        private final double diskSpaceWarningLevelRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        /**
         * 通过系统参数设置，默认值为 0.85 。如果磁盘分区使用超过该阈值，建议立即执行过期文件删除，但不会拒绝新消息的写入；
         */
        private final double diskSpaceCleanForciblyRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

        /*-------------------------------- RocketMQ 提供了两个与磁盘空间使用率相关的系统级参数，如下：-------------------*/


        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        /**
         * 是否立即清理文件，根据磁盘文件使用情况更新该值
         */
        private volatile boolean cleanImmediately = false;

        /**
         * 设置手动删除
         */
        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        /**
         * 清理 CommitLog 过期文件。整个执行过程分为两个大的步骤：
         * 1 尝试删除过期文件
         * 2 重试删除被 hang 住的文件（由于被其它线程引用，在第一步中未删除的文件），再重试一次
         */
        public void run() {
            try {

                // 清理过期文件
                this.deleteExpiredFiles();

                // 重试没有删除成功的过期文件(在拒绝被删除保护期 destroyMapedFileIntervalForcibly 内）
                this.redeleteHangedFile();

            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 清理过期文件：
         * 默认过期时间为72小时也就是3天，除了我们自动清理，下面几种情况也会自动清理 无论文件是否被消费过都会被清理
         * 1 默认是凌晨4点，自动清理过期时间的文件
         * 2 文件过期 磁盘空间占用率超过75%后，无论是否到达清理时间 都会自动清理过期时间
         * 3 磁盘占用率达到清理阈值 默认85%后，按照设定好的清理规则(默认是时间最早的)清理文件，无论是否过期
         * 4 磁盘占用率达到90%后，broker拒绝消息写入
         */
        private void deleteExpiredFiles() {
            int deleteCount = 0;

            // 文件保存的时长（从最后一次更新时间到现在），默认 72 小时。如果超过了该时间，则认为是过期文件。
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();

            // 删除物理文件的间隔时间，在一次清除过程中，可能需要被删除的文件不止一个，该值指定了删除一个文件后，休息多久再删除第二个
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();

            // 在清除过期文件时，如果该文件被其他线程占用（引用次数大于 0，比如读取消息），此时会阻止此次删除任务，同时在第一次试图删除该文件时记录当前时间戳，
            // 该值表示第一次拒绝删除之后能保留文件的最大时间，在此时间内，同样地可以被拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            /*------------- RocketMQ 在如下三种情况任意满足之一的情况下将执行删除文件操作 ------------*/
            /*1. 到了删除文件的时间点，RocketMQ 通过 deleteWhen 设置一天的固定时间执行一次删除过期文件操作，默认为凌晨 4 点  */
            /*2. 判断磁盘空间是否充足，如果不充足，则返回 ture，表示应该触发过期文件删除操作                               */
            /*3. 预留，手工触发；可以通过调用 excuteDeleteFilesManualy 方法手工触发过期文件删除                            */

            // 1 清理时间达到，默认为每天凌晨 4 点
            boolean timeup = this.isTimeToDelete();

            // 2 todo 磁盘空间是否要满了， 占用率默认为 75%
            boolean spacefull = this.isSpaceToDelete();

            // 3 手动可删除次数 > 0
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            // 达到以上条件任何一个
            if (timeup || spacefull || manualDelete) {

                // 如果是手动删除，那么递减可手动删除次数
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 是否立即删除
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                // 按天为单位
                fileReservedTime *= 60 * 60 * 1000;

                // todo 清理 CommitLog 文件，从开始到倒数第二个文件的范围内清理
                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(
                        fileReservedTime,
                        deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly,
                        cleanAtOnce);

                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 重试没有删除成功的过期文件
         */
        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();


            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;

                // 第一次拒绝删除之后能保留文件的最大时间
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

                // 尝试删除第一个文件
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 时间是否达到，默认每天凌晨 4 点
         *
         * @return
         */
        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 磁盘空间是否充足
         *
         * @return
         */
        private boolean isSpaceToDelete() {

            // diskMaxUsedSpaceRatio 表示 CommitLog 文件，ConsumeQueue 文件所在磁盘分区的最大使用量，默认为 75 ，如果超过该值，则需要立即清除过期文件
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 是否需要立即执行清除文件的操作，和文件过期时间无关了
            cleanImmediately = false;

            {
                // 当前 CommitLog 目录所在的磁盘分区的磁盘使用率 （commitlog 已经占用的存储容量/commitlog 目录所在磁盘分区总的存储容量）
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());


                // todo 判断磁盘是否可用，用当前已使用物理磁盘率 physicRatio、diskSpaceWarningLevelRatio、diskSpaceWarningLevelRatio 进行判断，
                // 如果当前磁盘使用率达到上述阈值，将返回 true 表示磁盘已满，需要进行文件删除。


                // 如果当前磁盘分区使用率大于 diskSpaceWarningLevelRatio 0.90 ，应该立即启动过期文件删除操作
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    // 设置立即启动文件删除
                    cleanImmediately = true;

                    // 如果当前磁盘分区使用率大于 diskSpaceCleanForciblyRatio 0.85，建议立即启动文件删除操作。
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;

                    // 如果当前磁盘使用率低于 diskSpaceCleanForciblyRatio ，将恢复磁盘可写
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            /*------------------------- todo 处理消息队列文件，逻辑同上 --------------------------*/
            {
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());


                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }


            // 默认没有满
            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }

        public boolean isSpaceFull() {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }


    /**
     * consumeQueue 过期文件删除任务 & index 无效文件删除任务
     */
    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * todo 根据 CommitLog 中最小的消息物理偏移量来删除无效的 ConsumeQueue 文件
         */
        private void deleteExpiredFiles() {
            // 1 删除 ConsumeQueue 文件的间隔时间
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            // 2 获取 CommitLog 最小物理偏移量
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();

            // 3 CommitLog 最小物理偏移量 > lastPhysicalMinOffset
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                // 获取内存 ConsumeQueue
                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    // 遍历所有的 ConsumeQueue 内存文件
                    for (ConsumeQueue logic : maps.values()) {

                        // 根据 CommitLog 最小物理偏移量作为标准，从倒数第 2 个遍历当前 ConsumeQueue 的所有内存文件，然后以每个文件中最后一个消息索引条目中记录的消息物理偏移量和 CommitLog 最小物理偏移量对比，
                        // 如果比 CommitLog 最小物理偏移量小，说明该文件是无效的，应该删除它。
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        // 删除一个 ConsumeQueue 歇息一会
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                // todo 依据 CommitLog 最小物理偏移量，删除无效的 index 文件
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * ConsumeQueue 刷盘线程任务
     * todo 说明：
     * 1 刷盘 ConsumeQueue 类似 CommitLog ，但是 ConsumeQueue 只有异步刷盘
     * 2 ConsumeQueue 刷盘任务基本和 CommitLog 异步刷盘一致，可参考 CommtLog 刷盘逻辑
     *
     * @see CommitLog.FlushRealTimeService
     * <p>
     * flush ConsumeQueue
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        /**
         * 最后 flush 时间戳
         */
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            // 最少刷盘页数
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            // retryTimes == RETRY_TIMES_OVER时，进行强制flush。主要用于shutdown时。
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            // 当时间满足flushConsumeQueueThoroughInterval时，即使写入的数量不足flushConsumeQueueLeastPages，也进行flush
            long logicsMsgTimestamp = 0;
            // 刷盘的间隔时间
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();

            // 每 flushConsumeQueueThoroughInterval 周期，执行一次 flush 。因为不是每次循环到都能满足 flushConsumeQueueLeastPages 大小，因此，需要一定周期进行一次强制 flush 。
            // 当然，不能每次循环都去执行强制 flush，这样性能较差。
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            // flush 消费队列
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            // 遍历存储中心缓存的消费队列，依次 flush ConsumeQueue
            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {

                        // todo flush ConsumeQueue(消费队列)
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            // todo flush 时保存 check point
            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        /**
         * flush ConsumeQueue  任务
         * <p>
         * 每 1000ms 执行一次 flush。
         */
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每 flushConsumeQueueThoroughInterval 周期，执行一次 flush
                    // 因为不是每次循环到都能满足 flushConsumeQueueLeastPages 大小，因此，需要一定周期进行一次强制 flush 。当然，不能每次循环都去执行强制 flush，这样性能较差。
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    // 等待 interval 时间
                    this.waitForRunning(interval);

                    // 执行 flush consumequeue
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Broker 关闭时，要强制刷新
            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 说明：重放消息线程任务
     * 1 该服务不断生成 消息位置信息 到 消费队列(ConsumeQueue)
     * 2 该服务不断生成 消息索引 到 索引文件(IndexFile)
     * 特别说明：
     * 1 reputFromOffset 不断指向下一条消息，生成 ConsumeQueue 和 IndexFile 对应的内容
     * 2 reputFromOffset 指向 BLANK ，即文件尾时，则指向下一个 MappedFile
     * <p>
     * RocketMQ 采用专门的线程来根据 commitlog offset 来将 commitlog 转发给 ConsumeQueue 和 IndexFile 。
     *
     * @see DefaultMessageStore#start()  在 Broker 启动时启动该任务
     */
    class ReputMessageService extends ServiceThread {

        /**
         * 重放消息的 CommitLog 物理偏移：
         * 1）该值的初始化值是在 Broker 启动时赋予的，是个绝对正确的物理偏移量。
         * 2）随着重放消息的过程，该值会不断推进
         */
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 剩余需要重放消息字节数
         *
         * @return
         */
        public long behind() {
            // CommitLog 最大的写入位置 - 要开始重放消息的 CommitLog 位置
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * 是否 CommitLog 可以重放消息
         *
         * @return
         */
        private boolean isCommitLogAvailable() {
            // 只要重放的位置没有达到 CommitLog 最大的写位置就可以
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 重放消息逻辑，不断生成 消息位置信息 到 消费队列(ConsumeQueue)
         */
        private void doReput() {
            // 如果从 commitlog 中开始拉取的偏移量小于最小偏移量，则更新这个偏移量
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                        this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                // 从有效位置开始
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }

            // todo 是否可以继续重放 CommitLog，原则是重放的偏移量 reputFromOffset  不能超出 CommitLog 最大偏移量
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // todo 是否允许重复转发，如果允许重复转发，则重放的偏移量 reputFromOffset 也不能 >= CommitLog 文件的提交指针
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);

                // 找到数据
                if (result != null) {
                    try {

                        // 更新 reputFromOffset ，以实际拉取消息的起始偏移量为准，即 this.fileFromOffset + reputFromOffset % mappedFileSize
                        this.reputFromOffset = result.getStartOffset();

                        // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条，创建 DispatchRequest 对象
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            // 2 尝试构建转发请求对象 DispatchRequest，即生成重放消息调度请求，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                            // todo 主要是从Nio ByteBuffer中，根据 commitlog 消息存储格式，解析出消息的核心属性，其中延迟消息的时间计算也在该逻辑中
                            // todo 注意：生成重放消息调度请求 (DispatchRequest) ，从 SelectMappedBufferResult 中读取一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息。
                            //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);


                            // 消息长度
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            // 读取成功，进行该条消息的派发
                            if (dispatchRequest.isSuccess()) {

                                // 解析得到的消息程度大于 0
                                if (size > 0) {

                                    // 3 todo 转发 DispatchRequest，根据 comitlog 文件内容实时构建 consumequeue、index文件
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // todo 4 当 Broker 是主节点 && Broker 开启的是长轮询，则通知消费队列有新的消息到达，这样处于等待中拉取消息请求可以再次拉取消息
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {

                                        // 在消息拉取长轮询模式下的消息达到监听器
                                        DefaultMessageStore.this.messageArrivingListener.arriving(
                                                dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(),
                                                dispatchRequest.getConsumeQueueOffset() + 1, // 消息在消息队列的逻辑偏移量
                                                dispatchRequest.getTagsCode(),
                                                dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(),
                                                dispatchRequest.getPropertiesMap());
                                    }

                                    // todo 更新下次重放消息的偏移量
                                    this.reputFromOffset += size;

                                    // todo 累加读取大小，判断是否读取完毕以及控制每次读取一条消息
                                    readSize += size;

                                    // 统计
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .addAndGet(dispatchRequest.getMsgSize());
                                    }

                                    // 对应的是 Blank ，即读取到 MappedFile 文件尾，跳转指向下一个 MappedFile
                                } else if (size == 0) {
                                    // 获取下次读取偏移量
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }

                                // 读取失败。这种场景基本是一个 Bug
                            } else if (!dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                            DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }

                    // 没有找到数据，结束 doReput 方法，等待下次继续执行
                } else {
                    doNext = false;
                }
            }
        }

        /**
         * 重放消息线程任务。每执行一次任务推送，休息 1ms 后继续尝试推送消息到 ConsumeQueue 和 Index 文件中
         * 即：不断生成消息位置信息到消费队列 ConsumeQueue 和 消息索引到索引文件 IndexFile
         */
        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {

                    // 每休息 1 毫秒，就继续抢占 CPU ，可以认为是实时的
                    Thread.sleep(1);

                    // 每处理一次 doReput() 方法，休眠 1 毫秒，基本上是一直在转发 commitlog 中的内容到 consumequeue、index
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
