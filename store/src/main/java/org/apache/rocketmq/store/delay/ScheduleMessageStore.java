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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.schedule.ScheduleMessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.delay.config.ScheduleMessageStoreConfig;
import org.apache.rocketmq.store.delay.config.ScheduleStorePathConfigHelper;
import org.apache.rocketmq.store.delay.tool.DirConfigHelper;
import org.apache.rocketmq.store.delay.wheel.MemoryIndex;
import org.apache.rocketmq.store.delay.wheel.ScheduleTimeWheel;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ 存储核心类，Broker 持有。包含了很多对存储文件进行操作的 API，其他模块对消息实体的操作都是通过该类进行的。
 * 说明：
 */
public class ScheduleMessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息存储配置属性
     * 存储相关的配置，如存储路径、commitLog 文件大小、刷盘频次等
     */
    private final ScheduleMessageStoreConfig messageStoreConfig;
    /**
     * ScheduleLog 的管理器
     */
    private final ScheduleLogManager scheduleLogManager;
    /**
     * CommitLog 过期文件清除线程任务
     */
    private final CleanCommitLogService cleanCommitLogService;
    /**
     * 存储统计服务
     */
    private final StoreStatsService storeStatsService;
    /**
     * 存储服务状态
     */
    private final RunningFlags runningFlags = new RunningFlags();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    /**
     * Broker 统计服务
     */
    private final BrokerStatsManager brokerStatsManager;
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

    private RandomAccessFile lockFile;
    private FileLock lock;
    private final SystemClock systemClock = new SystemClock();


    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    /**
     * 默认的消息存储
     */
    private final MessageStore messageStore;

    private final FlushRealTimeService flushRealTimeService;
    private final ScanReachScheduleLog scanReachScheduleLog;

    private final HashedWheelTimer hashedWheelTimer = ScheduleTimeWheel.INSTANCE.getWheelTimer();


    /**
     * 存储核心类，集中存储相关的核心类
     *
     * @param brokerConfig
     * @throws IOException
     */
    public ScheduleMessageStore(BrokerStatsManager brokerStatsManager, BrokerConfig brokerConfig, MessageStore messageStore) throws IOException {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = new ScheduleMessageStoreConfig();
        this.brokerStatsManager = brokerStatsManager;

        // 初始化一个 ScheduleLog
        this.scheduleLogManager = new ScheduleLogManager(this);
        this.cleanCommitLogService = new CleanCommitLogService();
        this.storeStatsService = new StoreStatsService();

        // ScheduleLog 异步刷盘任务
        flushRealTimeService = new FlushRealTimeService();
        // 定时扫描 ScheduleLog 加载到内存时间轮任务
        scanReachScheduleLog = new ScanReachScheduleLog();

        // 原始的存储服务
        this.messageStore = messageStore;

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
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
     */
    public boolean load() {
        boolean result = true;

        try {
            // 1 判断 user.home/store/abort 文件是否存在
            // Broker 在启动过时创建，在退出时通过注册 JVM 钩子函数删除 abrot 文件。也就是如果该文件存在，说明不是正常的关闭。
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // 4 加载 ConsumeQueue 文件
            result = result && this.loadScheduleLog();

            // 以上的文件都加载成功后，加载并存储 checkpoint 文件
            if (result) {
                // 5 文件存储检测点封装对象
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // 7 根据 Broker 是否正常停止，选择不同策略进行文件恢复
                // todo 对前面加载的 ScheduleLog 进行恢复
                this.recover(lastExitOK);
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        return result;
    }

    public long now() {
        return this.systemClock.now();
    }


    /**
     * todo 非常重要
     * 存储服务启动会启动多个后台线程
     *
     * @throws Exception
     */
    public void start() throws Exception {
        // todo 启动
        this.flushRealTimeService.start();
        this.scanReachScheduleLog.start();

        this.storeStatsService.start();
        this.createTempFile();

        // todo  开启系列定时任务，其中包含用来清理过期文件
        this.shutdown = false;
    }

    /**
     * 根据消息的物理偏移量和消息大小查找消息
     *
     * @param scheduleLog
     * @param scheduleLogOffset
     * @param size
     * @return
     */
    public MessageExt lookMessageByOffset(ScheduleLog scheduleLog, long scheduleLogOffset, int size) {
        SelectMappedBufferResult sbr = scheduleLog.getMessage(scheduleLogOffset, size);
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
     * 刷盘任务
     * 说明：MappedFile 落盘方式：
     * 1 开启使用堆外内存：数据先追加到堆外内存 -> 提交堆外内存数据到与物理文件的内存映射中 -> 物理文件的内存映射 flush 到磁盘
     * 2 没有开启使用堆外内存：数据直接追加到与物理文件直接映射的内存中 -> 物理文件的内存映射 flush 到磁盘
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }


    /*---------- 定时扫描 ScheduleLog 中的快要到期消息  ----------*/
    class ScanReachScheduleLog extends ServiceThread {
        @Override
        public void run() {
            // Broker 不关闭
            while (!this.isStopped()) {

                // 每隔 5s 扫描一次
                long interval = 1000 * 5;
                this.waitForRunning(interval);

                // 当前时间属于哪个分区文件
                Long dirNameByMills = DirConfigHelper.getDirNameByMills(System.currentTimeMillis());
                ScheduleLog scheduleLog = scheduleLogManager.getScheduleLogTable().get(dirNameByMills);
                if (scheduleLog == null || scheduleLog.getMappedFileQueue() == null) {
                    this.waitForRunning(interval);

                } else {

                    // 当前分区已经加载到内存时间轮的 ScheduleLog 的偏移量
                    ConcurrentMap<Long, Long> scheduleLogMemoryIndexTable = scheduleLogManager.getScheduleLogMemoryIndexTable();
                    Long offset = scheduleLogMemoryIndexTable.get(dirNameByMills);
                    if (offset == null) {
                        offset = 0L;
                    }
                    long memoryIndex = offset;
                    // 当前分区最大的物理偏移量
                    long maxOffset = scheduleLog.getMaxOffset();

                    // todo 读取当前 ScheduleLog 下的所有文件，并加载到内存时间轮
                    for (boolean doNext = true; offset <= maxOffset && doNext; ) {
                        // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                        SelectMappedBufferResult result = scheduleLogManager.getData(scheduleLog.getMappedFileQueue(), offset);
                        // 找到数据
                        if (result != null) {
                            try {

                                // 更新 reputFromOffset ，以实际拉取消息的起始偏移量为准，即 this.fileFromOffset + reputFromOffset % mappedFileSize
                                offset = result.getStartOffset();

                                // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条，创建 DispatchRequest 对象
                                for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                                    // 2 尝试构建转发请求对象 DispatchRequest，即生成重放消息调度请求，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                                    // todo 主要是从Nio ByteBuffer中，根据 ScheduleLog 消息存储格式，解析出消息的核心属性，其中延迟消息的时间计算也在该逻辑中
                                    // todo 注意：生成重放消息调度请求 (DispatchRequest) ，从 SelectMappedBufferResult 中读取一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息。
                                    //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                                    DispatchRequest dispatchRequest =
                                            scheduleLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                                    // 消息长度
                                    int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                                    // 读取成功，进行该条消息的派发
                                    if (dispatchRequest.isSuccess()) {
                                        // 解析得到的消息程度大于 0
                                        if (size > 0) {
                                            // todo 更新下次重放消息的偏移量
                                            offset += size;
                                            // todo 累加读取大小，判断是否读取完毕以及控制每次读取一条消息
                                            readSize += size;
                                            long pyOffset = dispatchRequest.getCommitLogOffset();
                                            String triggerTimeString = dispatchRequest.getPropertiesMap().get(ScheduleMessageConst.PROPERTY_DELAY_TIME);
                                            Long triggerTime = Long.parseLong(triggerTimeString);

                                            // 判断当前 ScheduleLog 是否已经加入到时间轮中
                                            if (!scheduleLogMemoryIndexTable.isEmpty() && memoryIndex > dispatchRequest.getCommitLogOffset()) {
                                                continue;
                                            }
                                            // 已经投递过了，无需重复投递
                                            // 这里使用 > ，可能相同时间多个消息
                                            // 当前分区已经投递到 CommitLog 中的消息的最大延时时间
                                            ConcurrentMap<Long, Long> scheduleDelayTimeTable = scheduleLogManager.getScheduleDelayTimeTable();
                                            Long delayTimeMills = scheduleDelayTimeTable.get(dirNameByMills);
                                            if (delayTimeMills == null) {
                                                delayTimeMills = 0L;
                                            }

                                            if (!scheduleDelayTimeTable.isEmpty() && delayTimeMills > triggerTime) {
                                                continue;
                                            }
                                            // 构建内存索引
                                            long diff = triggerTime - System.currentTimeMillis();
                                            MemoryIndex memoryIndexObj = new MemoryIndex(scheduleLogManager.getScheduleMessageStore(), triggerTime, pyOffset, size);
                                            // 过期的立即触发
                                            hashedWheelTimer.newTimeout(memoryIndexObj, diff < 0 ? 0 : diff, TimeUnit.MILLISECONDS);

                                            System.out.println(DirConfigHelper.getCurrentDateTime() + " 定时任务扫描延时消息文件，加载延时任务到时间轮，还有 " + (triggerTime - System.currentTimeMillis()) + " 毫秒触发延时任务！");


                                            // 加入时间轮成功后，更新加入到时间轮的最大偏移量，便于过滤后续的延时消息
                                            memoryIndex = dispatchRequest.getCommitLogOffset();
                                            scheduleLogMemoryIndexTable.put(dirNameByMills, memoryIndex);

                                            // 对应的是 Blank ，即读取到 MappedFile 文件尾，跳转指向下一个 MappedFile
                                        } else if (size == 0) {
                                            // 获取下次读取偏移量
                                            offset = scheduleLog.rollNextFile(offset);
                                            readSize = result.getSize();
                                        }

                                        // 读取失败。这种场景基本是一个 Bug
                                    } else if (!dispatchRequest.isSuccess()) {
                                        if (size > 0) {
                                            log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", offset);
                                            offset += size;
                                        } else {
                                            doNext = false;
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

                    // 更新加载到内存时间轮的消息物理偏移量
                    scheduleLogMemoryIndexTable.put(dirNameByMills, offset);
                }
            }
        }

        @Override
        public String getServiceName() {
            return ScanReachScheduleLog.class.getSimpleName();
        }
    }




    /*-------------------------------- 异步刷盘 -----------------------------*/

    /**
     * 异步刷盘 && 关闭内存字节缓冲区
     * 说明：实时 flush 线程服务，调用 MappedFile#flush 相关逻辑
     */
    class FlushRealTimeService extends FlushCommitLogService {
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
                // 每次循环是固定周期还是等待唤醒
                boolean flushCommitLogTimed = messageStoreConfig.isFlushCommitLogTimed();

                // 任务执行的时间间隔，默认 500ms
                int interval = messageStoreConfig.getFlushIntervalCommitLog();

                // 一次刷盘任务至少包含页数，如果待刷盘数据不足，小于该参数配置的值，将忽略本次刷盘任务，默认 4 页
                // todo 调成 0 ，立即刷盘
                int flushPhysicQueueLeastPages = messageStoreConfig.getFlushCommitLogLeastPages();

                // 两次真实刷盘的最大间隔时间，默认 10s
                int flushPhysicQueueThoroughInterval =
                        messageStoreConfig.getFlushCommitLogThoroughInterval();

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
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }


                    // todo 调用 MappedFile 进行 flush
                    long begin = System.currentTimeMillis();

                    for (ScheduleLog scheduleLog : scheduleLogManager.getScheduleLogTable().values()) {
                        scheduleLog.flush(flushPhysicQueueLeastPages);
                    }

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
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                for (ScheduleLog scheduleLog : scheduleLogManager.getScheduleLogTable().values()) {
                    result = scheduleLog.flush(0);
                }
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


            this.storeStatsService.shutdown();

            this.flushRealTimeService.shutdown();

            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();
        }


        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
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

        // 存储任意延时消息
        CompletableFuture<PutMessageResult> putResultFuture = this.scheduleLogManager.asyncPutMessage(msg);

        // todo 投递延时消息
        putResultFuture.whenComplete((a, b) -> {
            // 刷盘成功，判断是否要加载到时间轮
            String delayTime = msg.getProperty(ScheduleMessageConst.PROPERTY_DELAY_TIME);

            // todo 判断是否可以加载到时间轮，小于 30 分钟的加入时间轮
            long diff = Long.parseLong(delayTime) - System.currentTimeMillis();
            if (diff < DirConfigHelper.TRIGGER_TIME) {
                TimerTask timerTask = timeout -> {

                    // 记录触发的延时时间
                    String commitMills = msg.getProperties().get(ScheduleMessageConst.PROPERTY_DELAY_TIME);

                    // 清除延时消息标记
                    msg.getProperties().remove(ScheduleMessageConst.PROPERTY_DELAY_TIME);

                    // 发送消息到 CommitLog
                    PutMessageResult putMessageResult = messageStore.putMessage(msg);

                    // 如果发送成功，则继续下一个消息索引的获取与判断是否到期
                    if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {

                        System.out.println("添加消息场景触发了时间调度。msg: " + msg);

                        // todo 记录投递成功的物理偏移量，需要持久化
                        scheduleLogManager.getScheduleDelayTimeTable().put(DirConfigHelper.getDirNameByMills(Long.parseLong(delayTime)), Long.parseLong(commitMills));
                        return;

                        // 消息投递失败
                    } else {
                        // FIXME 重试
                        return;
                    }
                };

                // 记录 scheduleLog 的物理偏移量的消息进入时间轮
                scheduleLogManager.getScheduleLogMemoryIndexTable().put(DirConfigHelper.getDirNameByMills(Long.parseLong(delayTime)), msg.getCommitLogOffset());

                System.out.println("添加延时消息触发时间轮，还有 " + diff + "毫秒任务会被触发！");
                ScheduleTimeWheel.INSTANCE.getWheelTimer().newTimeout(timerTask, diff, TimeUnit.MILLISECONDS);
            }

        });

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


    /**
     * 判断操作系统PageCache是否繁忙，如果忙，则返回true。
     *
     * @return
     */

    public boolean isOSPageCacheBusy() {
        // 消息开始写入 Commitlog 文件时加锁的时间
//        // @see org.apache.rocketmq.store.CommitLog.putMessage
//        long begin = this.getCommitLog().getBeginTimeInLock();
//
//        // 一次消息追加过程中截止到当前持有锁的总时长，完成后重置为 0
//        // 即往内存映射文件或pageCache追加一条消息直到现在所耗时间
//        long diff = this.systemClock.now() - begin;
//
//        // 如果一次消息追加过程的时间超过了Broker配置文件osPageCacheBusyTimeOutMills ，默认 1000 即 1s，则认为pageCache繁忙，
//        return diff < 10000000
//                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();

        return false;
    }


    public SystemClock getSystemClock() {
        return systemClock;
    }


    /**
     * 获取消息
     *
     * @param scheduleLog 时间分区对应的内存映射文件
     * @param pyoffset    消息在 ScheduleLog 中的物理偏移量
     * @param msgSize     消息大小
     * @return
     */
    public GetMessageResult getMessage(
            final ScheduleLog scheduleLog,
            final long pyoffset,
            final int msgSize) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }
        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        // 当前消息队列最小偏移量
        long minOffset = 0;
        // 当前消息队列最大偏移量
        long maxOffset = 0;

        // 拉取消息的结果
        GetMessageResult getResult = new GetMessageResult();

        // 有了消息物理偏移量和消息大小，就可以从 CommitLog 文件中读取消息，根据偏移量与消息大小
        SelectMappedBufferResult selectResult = scheduleLog.getMessage(pyoffset, msgSize);
        if (null == selectResult) {
            // 从commitLog无法读取到消息，说明该消息对应的文件（MappedFile）已经删除，计算下一个MappedFile的起始位置
            if (getResult.getBufferTotalSize() == 0) {
                status = GetMessageStatus.MESSAGE_WAS_REMOVING;
            }
        }

        // 设置返回结果：设置下一次拉取偏移量，然后返回拉取结果
        getResult.addMessage(selectResult);
        getResult.setStatus(status);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;

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
        return ScheduleMessageStore.this.getMessageStoreConfig().getStorePathScheduleLog();
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
    private boolean loadScheduleLog() {
        // 封装 $user.home/store/schedulelog 文件夹
        File dirLogic = new File(ScheduleStorePathConfigHelper.getStorePathScheduleLog(this.messageStoreConfig.getStorePathRootDir()));
        // 获取 Store schedulelog 目录下所有子目录，加载进来
        File[] fileTimeList = dirLogic.listFiles();

        if (fileTimeList != null) {
            // 按照文件名进行排序 - 延时时间
            Arrays.sort(fileTimeList);

            // 遍历延时分区级别的目录
            for (File fileTime : fileTimeList) {

                // 文件夹对应的名 - 时间分区（如 1657278000000）
                String dirMills = fileTime.getName();
                ScheduleLog scheduleLog = new ScheduleLog(
                        Long.parseLong(dirMills),
                        ScheduleStorePathConfigHelper.getStorePathScheduleLog(this.messageStoreConfig.getStorePathRootDir()),
                        this.messageStoreConfig.getMaxMessageSize(),
                        this

                );

                scheduleLogManager.getScheduleLogTable().put(Long.parseLong(dirMills), scheduleLog);
                if (!scheduleLog.load()) {
                    return false;
                }
            }
        }

        log.info("load logics schedule all over, OK");
        return true;
    }

    /**
     * 文件恢复
     *
     * @param lastExitOK Broker 是否正常关闭
     */
    private void recover(final boolean lastExitOK) {
        // 恢复 ScheduleLog
        // 即移除非法的 offset
        scheduleLogManager.getScheduleLogTable().values().forEach(ScheduleLog::recoverNormally);

        // todo 恢复完尝试将最近时间粒度内的消息加载到时间轮中
        // 当前时间属于哪个分区文件
        Long dirNameByMills = DirConfigHelper.getDirNameByMills(System.currentTimeMillis());
        ScheduleLog scheduleLog = scheduleLogManager.getScheduleLogTable().get(dirNameByMills);
        if (scheduleLog == null || scheduleLog.getMappedFileQueue() == null) {
            return;
        } else {

            // 当前分区已经投递到 CommitLog 中的消息的最大延时时间
            ConcurrentMap<Long, Long> scheduleDelayTimeTable = scheduleLogManager.getScheduleDelayTimeTable();
            Long delayTimeMills = scheduleDelayTimeTable.get(dirNameByMills);
            if (delayTimeMills == null) {
                delayTimeMills = 0L;
            }

            // 当前分区已经加载到内存时间轮的 ScheduleLog 的偏移量
            ConcurrentMap<Long, Long> scheduleLogMemoryIndexTable = scheduleLogManager.getScheduleLogMemoryIndexTable();
            Long offset = scheduleLogMemoryIndexTable.get(dirNameByMills);
            if (offset == null) {
                offset = 0L;
            }
            long memoryIndex = offset;
            // 当前分区最大的物理偏移量
            long maxOffset = scheduleLog.getMaxOffset();

            // todo 读取当前 ScheduleLog 下的所有文件，并加载到内存时间轮
            for (boolean doNext = true; offset <= maxOffset && doNext; ) {
                // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                SelectMappedBufferResult result = scheduleLogManager.getData(scheduleLog.getMappedFileQueue(), offset);
                // 找到数据
                if (result != null) {
                    try {

                        // 更新 reputFromOffset ，以实际拉取消息的起始偏移量为准，即 this.fileFromOffset + reputFromOffset % mappedFileSize
                        offset = result.getStartOffset();

                        // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条，创建 DispatchRequest 对象
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            // 2 尝试构建转发请求对象 DispatchRequest，即生成重放消息调度请求，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                            // todo 主要是从Nio ByteBuffer中，根据 ScheduleLog 消息存储格式，解析出消息的核心属性，其中延迟消息的时间计算也在该逻辑中
                            // todo 注意：生成重放消息调度请求 (DispatchRequest) ，从 SelectMappedBufferResult 中读取一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息。
                            //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                            DispatchRequest dispatchRequest =
                                    scheduleLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                            // 消息长度
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            // 读取成功，进行该条消息的派发
                            if (dispatchRequest.isSuccess()) {
                                // 解析得到的消息程度大于 0
                                if (size > 0) {
                                    // todo 更新下次重放消息的偏移量
                                    offset += size;
                                    // todo 累加读取大小，判断是否读取完毕以及控制每次读取一条消息
                                    readSize += size;
                                    long pyOffset = dispatchRequest.getCommitLogOffset();
                                    String triggerTimeString = dispatchRequest.getPropertiesMap().get(ScheduleMessageConst.PROPERTY_DELAY_TIME);
                                    Long triggerTime = Long.parseLong(triggerTimeString);

                                    // 判断是否已经加入到时间轮中
                                    if (!scheduleLogMemoryIndexTable.isEmpty() && memoryIndex >= dispatchRequest.getCommitLogOffset()) {
                                        continue;
                                    }
                                    // 已经投递过了，无需重复投递
                                    // 这里使用 > ，可能相同时间多个消息
                                    if (!scheduleDelayTimeTable.isEmpty() && delayTimeMills > triggerTime) {
                                        continue;
                                    }

                                    // 还有多久触发
                                    long diff = triggerTime - System.currentTimeMillis();
                                    // 构建内存索引
                                    MemoryIndex memoryIndexObj = new MemoryIndex(scheduleLogManager.getScheduleMessageStore(), triggerTime, pyOffset, size);
                                    hashedWheelTimer.newTimeout(memoryIndexObj, diff < 0 ? 0 : diff, TimeUnit.MILLISECONDS);
                                    System.out.println(DirConfigHelper.getCurrentDateTime() + "  初始化加载延时任务文件到时间轮，还有 " + (diff) + " 毫秒触发");
                                    System.out.println();
                                    // 更新加入到时间轮的最大偏移量
                                    memoryIndex = dispatchRequest.getCommitLogOffset();
                                    scheduleLogMemoryIndexTable.put(dirNameByMills, memoryIndex);

                                    // 对应的是 Blank ，即读取到 MappedFile 文件尾，跳转指向下一个 MappedFile
                                } else if (size == 0) {
                                    // 获取下次读取偏移量
                                    offset = scheduleLog.rollNextFile(offset);
                                    readSize = result.getSize();
                                }

                                // 读取失败。这种场景基本是一个 Bug
                            } else if (!dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", offset);
                                    offset += size;
                                } else {
                                    doNext = false;
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

            // 更新加载到内存时间轮的消息物理偏移量
            scheduleLogMemoryIndexTable.put(dirNameByMills, offset);
        }

    }


    public ScheduleMessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }


    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public RunningFlags getRunningFlags() {
        return runningFlags;
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
            ScheduleMessageStore.log.info("executeDeleteFilesManually was invoked");
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

            } catch (Throwable e) {
                ScheduleMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
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
            long fileReservedTime = ScheduleMessageStore.this.getMessageStoreConfig().getFileReservedTime();

            // 删除物理文件的间隔时间，在一次清除过程中，可能需要被删除的文件不止一个，该值指定了删除一个文件后，休息多久再删除第二个
            int deletePhysicFilesInterval = ScheduleMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();

            // 在清除过期文件时，如果该文件被其他线程占用（引用次数大于 0，比如读取消息），此时会阻止此次删除任务，同时在第一次试图删除该文件时记录当前时间戳，
            // 该值表示第一次拒绝删除之后能保留文件的最大时间，在此时间内，同样地可以被拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除
            int destroyMapedFileIntervalForcibly = ScheduleMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

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
                boolean cleanAtOnce = ScheduleMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                // 按天为单位
                fileReservedTime *= 60 * 60 * 1000;

                // todo 清理 CommitLog 文件，从开始到倒数第二个文件的范围内清理
                for (ScheduleLog scheduleLog : scheduleLogManager.getScheduleLogTable().values()) {
                    deleteCount = scheduleLog.deleteExpiredFile(
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
            String when = ScheduleMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                ScheduleMessageStore.log.info("it's time to reclaim disk space, " + when);
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
            double ratio = ScheduleMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 是否需要立即执行清除文件的操作，和文件过期时间无关了
            cleanImmediately = false;

            {
                // 当前 CommitLog 目录所在的磁盘分区的磁盘使用率 （commitlog 已经占用的存储容量/commitlog 目录所在磁盘分区总的存储容量）
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());


                // todo 判断磁盘是否可用，用当前已使用物理磁盘率 physicRatio、diskSpaceWarningLevelRatio、diskSpaceWarningLevelRatio 进行判断，
                // 如果当前磁盘使用率达到上述阈值，将返回 true 表示磁盘已满，需要进行文件删除。


                // 如果当前磁盘分区使用率大于 diskSpaceWarningLevelRatio 0.90 ，应该立即启动过期文件删除操作
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        ScheduleMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    // 设置立即启动文件删除
                    cleanImmediately = true;

                    // 如果当前磁盘分区使用率大于 diskSpaceCleanForciblyRatio 0.85，建议立即启动文件删除操作。
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;

                    // 如果当前磁盘使用率低于 diskSpaceCleanForciblyRatio ，将恢复磁盘可写
                } else {
                    boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        ScheduleMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    ScheduleMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            /*------------------------- todo 处理消息队列文件，逻辑同上 --------------------------*/
            {
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(ScheduleMessageStore.this.getMessageStoreConfig().getStorePathRootDir());


                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        ScheduleMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        ScheduleMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    ScheduleMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
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
            double ratio = ScheduleMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                ScheduleMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    ScheduleMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = ScheduleMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    ScheduleMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }

    public ScheduleLogManager getScheduleLogManager() {
        return scheduleLogManager;
    }


    public static void main(String[] args) throws IOException {
        HashedWheelTimer wheelTimer = ScheduleTimeWheel.INSTANCE.getWheelTimer();

        wheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.println("xxxxx");
            }
        }, 10000, TimeUnit.MILLISECONDS);

        System.in.read();
    }
}
