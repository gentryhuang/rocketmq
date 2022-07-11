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

import io.netty.util.TimerTask;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.schedule.ScheduleMessageConst;
import org.apache.rocketmq.common.schedule.tool.ScheduleConfigHelper;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.delay.config.ScheduleMessageStoreConfig;
import org.apache.rocketmq.store.delay.config.ScheduleStorePathConfigHelper;
import org.apache.rocketmq.store.delay.wheel.ScheduleMemoryIndex;
import org.apache.rocketmq.store.delay.wheel.ScheduleTimeWheel;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
    private volatile boolean shutdown = true;

    /**
     * 文件刷盘检测点
     */
    private StoreCheckpoint storeCheckpoint;
    private AtomicLong printTimes = new AtomicLong(0);
    private RandomAccessFile lockFile;
    private final SystemClock systemClock = new SystemClock();
    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    /**
     * 默认的消息存储
     */
    private final MessageStore messageStore;
    /**
     * 周期性扫描时间分区文件（基于当前时间）
     */
    private final ScanReachScheduleLog scanReachScheduleLog;
    /**
     * 补偿扫描时间分区文件
     */
    private final ScanUnprocessedScheduleLog scanUnprocessedScheduleLog;


    /**
     * 存储核心类，集中存储相关的核心类
     *
     * @param brokerConfig
     * @throws IOException
     */
    public ScheduleMessageStore(BrokerStatsManager brokerStatsManager, BrokerConfig brokerConfig, MessageStore messageStore) throws IOException {
        this.messageStoreConfig = new ScheduleMessageStoreConfig();
        // 初始化一个 ScheduleLog
        this.scheduleLogManager = new ScheduleLogManager(this);
        this.cleanCommitLogService = new CleanCommitLogService();
        this.storeStatsService = new StoreStatsService();

        // 定时扫描 ScheduleLog 消息加载到内存时间轮任务
        scanReachScheduleLog = new ScanReachScheduleLog();
        scanUnprocessedScheduleLog = new ScanUnprocessedScheduleLog();
        // 原始的存储服务
        this.messageStore = messageStore;

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    /**
     * 加载磁盘文件到内存
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

    /**
     * todo 非常重要
     * 存储服务启动会启动多个后台线程
     *
     * @throws Exception
     * @link org.apache.rocketmq.broker.BrokerController#start()
     */
    public void start() throws Exception {
        // todo 启动
        this.scanReachScheduleLog.start();
        this.scanUnprocessedScheduleLog.start();

        this.storeStatsService.start();
        this.createTempFile();

        // todo  开启系列定时任务，其中包含用来清理过期文件
        this.addScheduleTask();

        // todo  开启系列定时任务，其中包含用来清理过期文件
        this.shutdown = false;
    }

    /**
     * 添加定时任务
     */
    private void addScheduleTask() {
        /**
         * todo 周期性清理 ScheduleLog 文件。默认每 10s 检查一次过期文件
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // 定期执行过期文件清理工作，默认 10s 执行一次。有三种情况会执行清理，具体看流程。
                ScheduleMessageStore.this.cleanCommitLogService.run();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);
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
     * 定时扫描 ScheduleLog 中的快要到期消息
     */
    class ScanReachScheduleLog extends ServiceThread {

        @Override
        public void run() {
            // 等待 5s
            waitForRunning(5000);

            // Broker 不关闭
            while (!this.isStopped()) {

                // 每隔 5s 扫描一次
                // todo FIXME 这个粒度需要根据添加延时消息时，多少时间内会放入时间轮。比如 5 分钟会放入，那么每隔 4-5分钟即可
                // todo 而补偿扫描的，可以是这个 5 分钟 - 0～5 时间内的时间


                // 当前时间属于哪个分区文件
                Long delayPartitionDirectory = ScheduleConfigHelper.getDelayPartitionDirectory(systemClock.now());

                // 获取最近分区文件
                ScheduleLog scheduleLog = scheduleLogManager.getScheduleLogTable().get(delayPartitionDirectory);
                if (scheduleLog == null || scheduleLog.getMappedFileQueue() == null) {
                    this.waitForRunning(ScheduleConfigHelper.TRIGGER_TIME);

                } else {

                    // 当前时间分区文件消息已经加载到内存时间轮的 ScheduleLog 的物理偏移量
                    ConcurrentMap<Long, Long> scheduleLogMemoryIndexTable = scheduleLogManager.getScheduleLogMemoryIndexTable();
                    Long offset = scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L);

                    // 当前时间分区文件消息已经投递到 CommitLog 中的消息的最大延时时间
                    ConcurrentMap<Long, Long> scheduleDelayTimeTable = scheduleLogManager.getScheduleDelayTimeTable();


                    // todo 读取当前 ScheduleLog 下的所有文件，并加载到内存时间轮
                    for (boolean doNext = true; isScheduleLogAvailable(offset, scheduleLog) && doNext; ) {
                        // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                        SelectMappedBufferResult result = scheduleLogManager.getData(scheduleLog.getMappedFileQueue(), offset);
                        // 找到数据
                        if (result != null) {
                            try {

                                // 更新拉取 ScheduleLog 的物理偏移量 ，以实际拉取消息的起始偏移量为准
                                offset = result.getStartOffset();

                                // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条
                                for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                                    // 2 尝试构建转发请求对象 DispatchRequest 来保证 ScheduleLog 消息的正确性，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                                    //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                                    DispatchRequest dispatchRequest =
                                            scheduleLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                                    // 消息大小
                                    int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                                    // 读取成功
                                    if (dispatchRequest.isSuccess()) {
                                        if (size > 0) {
                                            // 更新拉取 ScheduleLog 的物理偏移量
                                            offset += size;
                                            // 累加读取大小，用于判断是否读取完毕以及控制每次读取一条消息
                                            readSize += size;
                                            // 获取当前 ScheduleLog 消息所在的物理偏移量
                                            long pyOffset = dispatchRequest.getCommitLogOffset();

                                            // 获取消息中存储的延时执行时间
                                            Long triggerTime;
                                            try {
                                                triggerTime = Long.parseLong(dispatchRequest.getPropertiesMap().get(ScheduleMessageConst.PROPERTY_DELAY_TIME));
                                            } catch (Exception ex) {
                                                continue;
                                            }

                                            // 判断当前消息是否已经加入过时间轮中，物理偏移量肯定是唯一的
                                            if (!scheduleLogMemoryIndexTable.isEmpty() && scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L) >= dispatchRequest.getCommitLogOffset()) {
                                                continue;
                                            }
                                            // 已经投递过了，无需重复投递；这里使用 > ，可能相同时间多个消息
                                            if (!scheduleDelayTimeTable.isEmpty() && scheduleDelayTimeTable.getOrDefault(delayPartitionDirectory, 0L) > triggerTime) {
                                                continue;
                                            }

                                            // 还有多久触发
                                            long diff = triggerTime - systemClock.now();
                                            // todo 延时补偿粒度，太久的消息不处理，交给补偿线程任务。
                                            if (diff < 0) {
                                                continue;
                                            }

                                            // 过期的立即触发
                                            ScheduleMemoryIndex memoryIndexObj = new ScheduleMemoryIndex(scheduleLogManager.getScheduleMessageStore(), triggerTime, pyOffset, size);
                                            scheduleLog.getHashedWheelTimer().newTimeout(memoryIndexObj, diff, TimeUnit.MILLISECONDS);

                                            //   System.out.println(ScheduleConfigHelper.getCurrentDateTime() + " 定时任务扫描延时消息文件，加载延时任务到时间轮，还有 " + (triggerTime - System.currentTimeMillis()) + " 毫秒触发延时任务！");
                                            System.out.println(ScheduleConfigHelper.getCurrentDateTime() + " 定时任务扫描延时消息文件，加载延时任务到时间轮，msgID: " + dispatchRequest.getUniqKey());


                                            // 更新加入到时间轮的最大偏移量
                                            scheduleLogMemoryIndexTable.put(delayPartitionDirectory, dispatchRequest.getCommitLogOffset());

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

                            // 没有找到数据，等待下次继续执行
                        } else {
                            doNext = false;
                        }
                    }
                }

                // 扫描一次修改执行的时间
                this.waitForRunning(ScheduleConfigHelper.TRIGGER_TIME);
            }
        }

        @Override
        public String getServiceName() {
            return ScanReachScheduleLog.class.getSimpleName();
        }

        @Override
        protected void onWaitEnd() {
            // do noting
        }
    }


    /**
     * 定时补偿 ScheduleLog 中的过期的消息
     */
    class ScanUnprocessedScheduleLog extends ServiceThread {
        Random random = new Random();

        @Override
        public void run() {
            // 等待 1s
            waitForRunning(1000);

            // Broker 不关闭
            while (!this.isStopped()) {
                // 当前时间属于哪个分区文件
                Long delayPartitionDirectory = ScheduleConfigHelper.getDelayPartitionDirectory(compensateDelayTime(random));

                // 获取对应的分区文件
                ScheduleLog scheduleLog = scheduleLogManager.getScheduleLogTable().get(delayPartitionDirectory);
                if (scheduleLog == null || scheduleLog.getMappedFileQueue() == null) {
                    Thread.yield();
                } else {

                    // 当前时间分区文件消息已经加载到内存时间轮的 ScheduleLog 的物理偏移量
                    ConcurrentMap<Long, Long> scheduleLogMemoryIndexTable = scheduleLogManager.getScheduleLogMemoryIndexTable();
                    Long offset = scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L);

                    // 当前时间分区文件消息已经投递到 CommitLog 中的消息的最大延时时间
                    ConcurrentMap<Long, Long> scheduleDelayTimeTable = scheduleLogManager.getScheduleDelayTimeTable();


                    // todo 读取当前 ScheduleLog 下的所有文件，并加载到内存时间轮
                    for (boolean doNext = true; isScheduleLogAvailable(offset, scheduleLog) && doNext; ) {
                        // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                        SelectMappedBufferResult result = scheduleLogManager.getData(scheduleLog.getMappedFileQueue(), offset);
                        // 找到数据
                        if (result != null) {
                            try {

                                // 更新拉取 ScheduleLog 的物理偏移量 ，以实际拉取消息的起始偏移量为准
                                offset = result.getStartOffset();

                                // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条
                                for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                                    // 2 尝试构建转发请求对象 DispatchRequest 来保证 ScheduleLog 消息的正确性，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                                    //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                                    DispatchRequest dispatchRequest =
                                            scheduleLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                                    // 消息大小
                                    int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                                    // 读取成功
                                    if (dispatchRequest.isSuccess()) {
                                        if (size > 0) {
                                            // 更新拉取 ScheduleLog 的物理偏移量
                                            offset += size;
                                            // 累加读取大小，用于判断是否读取完毕以及控制每次读取一条消息
                                            readSize += size;
                                            // 获取当前 ScheduleLog 消息所在的物理偏移量
                                            long pyOffset = dispatchRequest.getCommitLogOffset();

                                            // 获取消息中存储的延时执行时间
                                            Long triggerTime;
                                            try {
                                                triggerTime = Long.parseLong(dispatchRequest.getPropertiesMap().get(ScheduleMessageConst.PROPERTY_DELAY_TIME));
                                            } catch (Exception ex) {
                                                continue;
                                            }

                                            // 判断当前消息是否已经加入过时间轮中
                                            if (!scheduleLogMemoryIndexTable.isEmpty() && scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L) >= dispatchRequest.getCommitLogOffset()) {
                                                continue;
                                            }
                                            // 已经投递过了，无需重复投递
                                            // 这里使用 > ，可能相同时间多个消息
                                            if (!scheduleDelayTimeTable.isEmpty() && scheduleDelayTimeTable.getOrDefault(delayPartitionDirectory, 0L) > triggerTime) {
                                                continue;
                                            }

                                            // 还有多久触发
                                            long diff = triggerTime - systemClock.now();
                                            // 补偿消息，一般来说是超时没有投递的，正常的消息交给定时扫描线程任务
                                            if (diff >= 0) {
                                                continue;
                                            }

                                            // 只补偿 [-1s ~ -5min] 的任务，否则打印消息可能丢失日志，交给业务方处理
                                            if (ScheduleConfigHelper.COMPENSATE_TIME > diff) {
                                                log.warn("this message is overdue {} mills seconds , maybe is can not commit message，message = {}", -diff, dispatchRequest);
                                                System.out.println("this message is overdue {} mills seconds , maybe is can not commit message，message = {}");
                                                continue;
                                            }

                                            ScheduleMemoryIndex memoryIndexObj = new ScheduleMemoryIndex(scheduleLogManager.getScheduleMessageStore(), triggerTime, pyOffset, size);
                                            // 过期的立即触发
                                            scheduleLog.getHashedWheelTimer().newTimeout(memoryIndexObj, 0, TimeUnit.MILLISECONDS);

                                            System.out.println(ScheduleConfigHelper.getCurrentDateTime() + " 补偿定时任务扫描延时消息文件，加载延时任务到时间轮，还有 " + (triggerTime - System.currentTimeMillis()) + " 毫秒触发延时任务！");

                                            // 更新加入到时间轮的最大偏移量
                                            scheduleLogMemoryIndexTable.put(delayPartitionDirectory, dispatchRequest.getCommitLogOffset());

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

                            // 没有找到数据，等待下次继续执行
                        } else {
                            doNext = false;
                        }
                    }

                }

                // 扫描一次后进入等待
                waitForRunning(ScheduleConfigHelper.TRIGGER_TIME);
            }
        }

        @Override
        public String getServiceName() {
            return ScanReachScheduleLog.class.getSimpleName();
        }

        @Override
        protected void onWaitEnd() {
            // do nothing
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
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();
        }


        if (lockFile != null) {
            try {
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
     * 是否可以继续拉取 ScheduleLog
     *
     * @return
     */
    private boolean isScheduleLogAvailable(long offset, ScheduleLog scheduleLog) {
        return offset < scheduleLog.getMaxOffset();
    }

    /**
     * 补偿指定时间粒度的扫描时间
     * <p>
     * todo FIXME
     *
     * @return
     */
    private long compensateDelayTime(Random random) {
        return systemClock.now() - random.nextInt((int) ScheduleConfigHelper.TRIGGER_TIME);
    }


    public long now() {
        return this.systemClock.now();
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

        // putResultFuture 执行完会回调该方法，但是执行线程不会等待，它会直接返回
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

            // 判断是否将延时消息加载到时间轮中
            putMessageToMemoryIndex(msg);
        });

        return putResultFuture;
    }

    /**
     * 对于快触发的消息，构建内存索引并加入时间轮
     *
     * @param msg 延时消息
     */
    private void putMessageToMemoryIndex(MessageExtBrokerInner msg) {
        try {
            // 刷盘成功，判断是否要加载到时间轮
            String delayTime = msg.getProperty(ScheduleMessageConst.PROPERTY_DELAY_TIME);
            long diff = Long.parseLong(delayTime) - this.getSystemClock().now();
            // 小于指定时间粒度的延时消息加入时间轮
            if (diff <= ScheduleConfigHelper.TRIGGER_TIME) {
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
                        scheduleLogManager.getScheduleDelayTimeTable().put(ScheduleConfigHelper.getDelayPartitionDirectory(Long.parseLong(delayTime)), Long.parseLong(commitMills));
                        return;

                        // 消息投递失败
                    } else {
                        // FIXME 重试，移除记录的 ScheduleLog 物理偏移量，重新加载到时间轮
                        scheduleLogManager.getScheduleLogMemoryIndexTable().remove(ScheduleConfigHelper.getDelayPartitionDirectory(Long.parseLong(delayTime)));
                        return;
                    }
                };

                // 记录 scheduleLog 的物理偏移量的消息进入时间轮
                scheduleLogManager.getScheduleLogMemoryIndexTable().put(ScheduleConfigHelper.getDelayPartitionDirectory(Long.parseLong(delayTime)), msg.getCommitLogOffset());
                ScheduleTimeWheel.INSTANCE.getWheelTimer().newTimeout(timerTask, diff, TimeUnit.MILLISECONDS);
            }
        } catch (Exception ex) {
            log.error("put schedule message to memory index error， ex={}", ex);
        }
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

    public SystemClock getSystemClock() {
        return systemClock;
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

                this.scheduleLogManager.getScheduleLogTable().put(Long.parseLong(dirMills), scheduleLog);
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
        // 恢复 ScheduleLog，即移除非法的 offset
        scheduleLogManager.getScheduleLogTable().values().forEach(ScheduleLog::recoverNormally);
        Long delayPartitionDirectory = ScheduleConfigHelper.getDelayPartitionDirectory(systemClock.now());

        // 恢复完尝试将最近时间分区文件中的消息加载到内存时间轮中以进行调度
        ScheduleLog scheduleLog = scheduleLogManager.getScheduleLogTable().get(delayPartitionDirectory);
        if (scheduleLog == null || scheduleLog.getMappedFileQueue() == null) {
            return;
        }

        try {

            // 当前时间分区文件消息已经投递到 CommitLog 中的消息的最大延时时间
            ConcurrentMap<Long, Long> scheduleDelayTimeTable = scheduleLogManager.getScheduleDelayTimeTable();

            // 当前时间分区文件消息已经加载到内存时间轮的 ScheduleLog 的物理偏移量
            ConcurrentMap<Long, Long> scheduleLogMemoryIndexTable = scheduleLogManager.getScheduleLogMemoryIndexTable();
            Long offset = scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L);

            // 读取当前 ScheduleLog 下的所有文件，并加载到内存时间轮
            for (boolean doNext = true; isScheduleLogAvailable(offset, scheduleLog) && doNext; ) {

                // 1 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该内存文件中有效数据的最大偏移量的数据
                SelectMappedBufferResult result = scheduleLogManager.getData(scheduleLog.getMappedFileQueue(), offset);
                // 找到数据
                if (result != null) {
                    try {
                        // 更新拉取 ScheduleLog 的物理偏移量 ，以实际拉取消息的起始偏移量为准
                        offset = result.getStartOffset();

                        // 从 result 返回的 ByteBuffer 中循环读取消息，一次读取一条
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            // 2 尝试构建转发请求对象 DispatchRequest 来保证 ScheduleLog 消息的正确性，请求里主要包含一条消息 (Message) 或者 文件尾 (BLANK) 的基本信息
                            //  怎么做到只有一条的呢？result.getByteBuffer() 的读取方法指针 & readSize 更新控制
                            DispatchRequest dispatchRequest =
                                    scheduleLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                            // 消息大小
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            // 读取成功
                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    // 更新拉取 ScheduleLog 的物理偏移量
                                    offset += size;
                                    // 累加读取大小，用于判断是否读取完毕以及控制每次读取一条消息
                                    readSize += size;

                                    // 获取当前 ScheduleLog 消息所在的物理偏移量
                                    long pyOffset = dispatchRequest.getCommitLogOffset();

                                    // 获取消息中存储的延时执行时间
                                    Long triggerTime;
                                    try {
                                        triggerTime = Long.parseLong(dispatchRequest.getPropertiesMap().get(ScheduleMessageConst.PROPERTY_DELAY_TIME));
                                    } catch (Exception ex) {
                                        continue;
                                    }

                                    // 判断当前消息是否已经加入过时间轮中
                                    if (!scheduleLogMemoryIndexTable.isEmpty() && scheduleLogMemoryIndexTable.getOrDefault(delayPartitionDirectory, 0L) >= dispatchRequest.getCommitLogOffset()) {
                                        continue;
                                    }
                                    // 已经投递过了，无需重复投递
                                    // 这里使用 > ，可能相同时间多个消息
                                    if (!scheduleDelayTimeTable.isEmpty() && scheduleDelayTimeTable.getOrDefault(delayPartitionDirectory, 0L) > triggerTime) {
                                        continue;
                                    }

                                    // 还有多久触发
                                    long diff = triggerTime - systemClock.now();
                                    // todo  FIXME 延时补偿粒度，补偿消息交给补偿线程任务
                                    // todo 删除消息文件，依据当前时间和文件夹名称，文件夹名 < 当前时间对应的时间分区，一定是过期的文件（过期扫描会往前补偿一个文件）
                                    if (0 > diff) {
                                        continue;
                                    }

                                    // 构建内存索引
                                    ScheduleMemoryIndex memoryIndexObj = new ScheduleMemoryIndex(scheduleLogManager.getScheduleMessageStore(), triggerTime, pyOffset, size);
                                    scheduleLog.getHashedWheelTimer().newTimeout(memoryIndexObj, diff, TimeUnit.MILLISECONDS);
                                    System.out.println(ScheduleConfigHelper.getCurrentDateTime() + "  初始化加载延时任务文件到时间轮，还有 " + (diff) + " 毫秒触发");
                                    System.out.println();


                                    // 更新加入到时间轮的最大偏移量
                                    scheduleLogMemoryIndexTable.put(delayPartitionDirectory, dispatchRequest.getCommitLogOffset());

                                    // 对应的是 Blank ，即读取到 MappedFile 文件尾，跳转指向下一个 MappedFile
                                } else if (size == 0) {
                                    // 获取下次读取偏移量
                                    offset = scheduleLog.rollNextFile(offset);
                                    readSize = result.getSize();
                                }

                                // 读取失败。这种场景基本是一个 Bug
                            } else if (!dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. offset={}", offset);
                                    offset += size;
                                } else {
                                    doNext = false;
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }

                    // 没有找到数据，等待下次继续执行
                } else {
                    doNext = false;
                }
            }

        } catch (Exception ex) {
            log.error("[BUG]org.apache.rocketmq.store.delay.ScheduleMessageStore.recover error!");
        }

    }


    /**
     * ScheduleLog  过期文件删除任务
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
         * 清理 ScheduleLog 过期文件。整个执行过程分为两个大的步骤：
         * 1 尝试删除过期文件 - 文件夹名 < 当前时间对应分区时间，就说明之前的文件夹下的所有消息已经到期了，可以删除了。
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

            // 删除物理文件的间隔时间，在一次清除过程中，可能需要被删除的文件不止一个，该值指定了删除一个文件后，休息多久再删除第二个
            int deletePhysicFilesInterval = ScheduleMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();

            // 在清除过期文件时，如果该文件被其他线程占用（引用次数大于 0，比如读取消息），此时会阻止此次删除任务，同时在第一次试图删除该文件时记录当前时间戳，
            // 该值表示第一次拒绝删除之后能保留文件的最大时间，在此时间内，同样地可以被拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除
            int destroyMapedFileIntervalForcibly = ScheduleMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            /*------------- RocketMQ 在如下三种情况任意满足之一的情况下将执行删除文件操作 ------------*/
            /*1. 到了删除文件的时间点，RocketMQ 通过 deleteWhen 设置一天的固定时间尝试执行一次删除过期文件操作，默认为凌晨 4 点  */
            /*2. 判断磁盘空间是否充足，如果不充足，则返回 ture，表示应该触发过期文件删除操作                                  */
            /*3. 预留，手工触发；可以通过调用 excuteDeleteFilesManualy 方法手工触发过期文件删除                            */

            // 1 清理时间达到，默认为每天凌晨 4 点
            boolean timeup = this.isTimeToDelete();

            // 2 磁盘空间是否要满了， 占用率默认为 75%
            boolean spacefull = this.isSpaceToDelete();

            // 3 手动可删除次数 > 0
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            // 达到以上条件任何一个
            if (timeup || spacefull || manualDelete) {

                // 如果是手动删除，那么递减可手动删除次数
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 当前时间对应时间分区
                // FIXME 加一个补偿机制 5 分钟，供补偿扫描线程执行。待优化
                Long delayPartitionDirectory = ScheduleConfigHelper.getDelayPartitionDirectory(systemClock.now() + ScheduleConfigHelper.COMPENSATE_TIME);

                // 顺序遍历，即从最早的文件夹遍历
                ConcurrentMap<Long, ScheduleLog> scheduleLogTable = scheduleLogManager.getScheduleLogTable();
                List<Long> fileTimes = scheduleLogManager.getScheduleLogTable().keySet().stream().sorted().collect(Collectors.toList());
                for (Long fileTime : fileTimes) {
                    // 是否立即删除
                    boolean forceCleanAll = false;

                    // 获取对应的 ScheduleLog
                    ScheduleLog scheduleLog = scheduleLogTable.get(fileTime);
                    if (scheduleLog == null) {
                        continue;
                    }

                    // 磁盘空间是否要满了
                    spacefull = this.isSpaceToDelete();

                    // 旧的文件夹可以删除了
                    if (delayPartitionDirectory > fileTime) {
                        forceCleanAll = true;
                    }

                    // 执行清理 ScheduleLog
                    deleteCount = scheduleLog.deleteExpiredFile(
                            deletePhysicFilesInterval,
                            destroyMapedFileIntervalForcibly,
                            forceCleanAll,
                            cleanImmediately);

                    if (deleteCount > 0) {
                        // 如果是清除整个文件夹，那么删除空的文件夹子
                        if (forceCleanAll && scheduleLog.getMappedFileQueue().getMappedFiles().isEmpty()) {

                            // todo 先清理缓存和进度，防止其它业务逻辑拿到 ScheduleLog 是无用的
                            cleaScheduleCache(scheduleLogManager, fileTime);

                            // 销毁 ScheduleLog 所有资源
                            scheduleLog.destroy();
                        }
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
                    // cleanImmediately = true;


                    // 如果当前磁盘分区使用率大于 diskSpaceCleanForciblyRatio 0.85，建议立即启动文件删除操作。
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    // cleanImmediately = true;

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

    /**
     * 清理 ScheduleLog 相关唤醒
     *
     * @param scheduleLogManager      ScheduleLog 管理器
     * @param delayPartitionDirectory 时间分区目录
     */
    private void cleaScheduleCache(ScheduleLogManager scheduleLogManager, Long delayPartitionDirectory) {
        // 清理 ScheduleLog 缓存
        scheduleLogManager.getScheduleLogTable().remove(delayPartitionDirectory);
        // 清理时间分区对应的最大投递消息到 CommitLog 的时间
        scheduleLogManager.getScheduleDelayTimeTable().remove(delayPartitionDirectory);
        // 清理时间分区对应的加载到时间轮的 ScheduleLog 的物理偏移量
        scheduleLogManager.getScheduleLogMemoryIndexTable().remove(delayPartitionDirectory);
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }

    public ScheduleLogManager getScheduleLogManager() {
        return scheduleLogManager;
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


}
