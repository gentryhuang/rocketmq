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
package org.apache.rocketmq.store.config;

import java.io.File;

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;

/**
 * 消息存储配置
 */
public class MessageStoreConfig {
    /**
     * Broker 的存储根目录，默认为 $user.home/store
     */
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    /**
     * CommitLog 的存储目录，默认为 $user.home/store/commitlog
     */
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store" + File.separator + "commitlog";

    /**
     * 如：每个 Commitlog MappedFile 文件的大小，默认为 1G
     */
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    /**
     * ConsumeQueue 存放的是定长的信息（20字节、偏移量、size、tagscode）
     * 默认 30w * ConsumeQueue.CQ_STORE_UNIT_SIZE
     */
    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    /**
     * 是否开启 consumeQueueExt ，默认为 false
     * 如果消费端消息消费速度跟不上，是否创建一个扩展的 ConsumeQueue 文件。如果不开启，应该会阻塞从 commitlog 文件中获取消息，
     * 并且 ConsumeQueue 应该是按 topic 独立的
     */
    private boolean enableConsumeQueueExt = false;

    /**
     * 扩展的 ConsumeQueue 文件大小，默认 48M
     */
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;


    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;

    /**
     * 刷写 CommitLog 到磁盘的间隔时间，默认 500 毫秒
     * RocketMQ 会启动后台线程，将消息刷写到磁盘，即 flush 操作，调用文件通道的 force() 方法
     */
    @ImportantField
    private int flushIntervalCommitLog = 500;

    /**
     * 提交消息到 CommitLog 对应的文件通道的间隔时间，原理与上面类似。将消息写入到文件通道（调用 FileChannel.write 方法）得到最新的写指针，默认为 200 毫秒。
     *
     * @see org.apache.rocketmq.store.CommitLog.CommitRealTimeService 执行间隔时间，默认 200ms
     */
    // Only used if TransientStorePool enabled 在开启 TransientStorePool 的情况下，将消息写入到 FileChannel
    // flush data to FileChannel
    @ImportantField
    private int commitIntervalCommitLog = 200;

    /**
     * 在 put message(将消息按格式封装成 msg 放入相关队列时使用的锁机制：自旋或 ReentrantLock)
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     * By default it is set to false indicating using spin lock when putting message.
     */
    private boolean useReentrantLockWhenPutMessage = false;

    /**
     * 默认是 false ，表示使用 await 方法等待；如果是 true ，表示使用 Thread.sleep 方法等待
     */
    // Whether schedule flush,default is real-time
    @ImportantField
    private boolean flushCommitLogTimed = false;

    /**
     * 刷写到 ConsumeQueue 的间隔，默认为 1s
     */
    private int flushIntervalConsumeQueue = 1000;


    /**
     * Resource reclaim interval
     * 清理资源的时间间隔，默认 10s
     */
    private int cleanResourceInterval = 10000;

    /**
     * CommitLog removal interval
     * CommitLog 删除时间间隔
     */
    private int deleteCommitLogFilesInterval = 100;

    /**
     * ConsumeQueue removal interval
     * ConsumeQueue 删除时间间隔
     */
    private int deleteConsumeQueueFilesInterval = 100;

    /**
     * 第一次拒绝删除之后能保留文件的最大时间，在此时间内，同样地可以被拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除
     */
    private int destroyMapedFileIntervalForcibly = 1000 * 120;

    /**
     * 由于在拒绝被删除保护期内的文件不能删除，可以开始删除的是时间端
     */
    private int redeleteHangedFileInterval = 1000 * 120;


    // 清理时间是凌晨 4 点
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";

    // 磁盘空间占用率
    private int diskMaxUsedSpaceRatio = 75;

    // 在删除日志文件之前保存该日志文件的小时数(以小时为单位)
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;

    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;
    // The maximum size of message,default is 4M
    private int maxMessageSize = 1024 * 1024 * 4;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    private boolean checkCRCOnRecover = true;

    /**
     * 每次 flush commitlog 时最小发生变化的页数
     */
    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;

    /**
     * 每次 commitlog 提交任务至少需要的页数
     */
    // How many pages are to be committed when commit data to file
    private int commitCommitLogLeastPages = 4;

    /**
     * 用字节 0 填充整个文件，每多少页刷盘一次，默认 4096 ，异步刷盘模式生效
     */
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    /**
     * 一次刷盘至少需要的脏页数量，默认为 2 ，针对 consumequeue 文件
     */
    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 1000 * 10;

    /**
     * 两次真实提交的最大间隔时间
     */
    private int commitCommitLogThoroughInterval = 200;
    private int flushConsumeQueueThoroughInterval = 1000 * 60;

    /**
     * 消息在内存中，最大传输大小，默认 256K
     */
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    /**
     * 消息在内存中，最大传输消息数量，默认 32
     */
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;

    /**
     * 消息在磁盘上的情况下，最大传输大小，默认 64K
     */
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    /**
     * 消息在磁盘上的情况下，最大传输消息数量，默认 8
     */
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    @ImportantField
    private boolean messageIndexSafe = false;
    /**
     * HA 监听端口
     */
    private int haListenPort = 10912;

    /**
     * 主服务器与从服务器的高可用心跳发送时间间隔默认为 5s
     */
    private int haSendHeartbeatInterval = 1000 * 5;

    /**
     * 连接保活 20s
     */
    private int haHousekeepingInterval = 1000 * 20;

    /**
     * 默认 32K
     */
    private int haTransferBatchSize = 1024 * 32;
    @ImportantField
    private String haMasterAddress = null;
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;
    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    private int syncFlushTimeout = 1000 * 5;

    /**
     * 消息延迟级别字符串配置
     * 18 个级别。注意:
     * - 可以配置自定义 messageDelayLevel
     * - 属于 broker 的属性，不属于某个 topic
     */
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    private long flushDelayOffsetInterval = 1000 * 10;
    @ImportantField
    private boolean cleanFileForciblyEnable = true;


    private boolean warmMapedFileEnable = false;
    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;
    private boolean duplicationEnable = false;
    private boolean diskFallRecorded = true;

    /**
     * 认为 OS pageCache 繁忙 的超时时间，即在一次消息追加过程中的据当前时间的耗时
     */
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;

    /**
     * 内存级别的读写分离机制
     * 优点：消息直接写入堆外内存，然后异步写入 pagecache ，相比每条消息追加直接写入 pagecache ，最大的优势是实现了批量化消息写入
     * 缺点：如果 Broker 进程异常退出，放入 pagecache 中的数据不会丢失，而存储在堆外内存数据会丢失
     */
    @ImportantField
    private boolean transientStorePoolEnable = false;
    private int transientStorePoolSize = 5;
    private boolean fastFailIfNoBufferInStorePool = false;

    private boolean enableDLegerCommitLog = false;
    private String dLegerGroup;
    private String dLegerPeers;
    private String dLegerSelfId;

    private String preferredLeaderId;

    private boolean isEnableBatchPush = false;

    private boolean enableScheduleMessageStats = true;

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMappedFileSizeConsumeQueue() {

        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }

    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    /**
     * 获取 CommitLog、ConsumeQueue 文件所在磁盘分区的最大使用量
     *
     * @return
     */
    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }

    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }

    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }

    public int getFileReservedTime() {
        return fileReservedTime;
    }

    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }

    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }

    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }

    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }

    public int getHaListenPort() {
        return haListenPort;
    }

    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }

    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }

    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }

    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }

    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }

    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }

    public int getHaSlaveFallbehindMax() {
        return haSlaveFallbehindMax;
    }

    public void setHaSlaveFallbehindMax(int haSlaveFallbehindMax) {
        this.haSlaveFallbehindMax = haSlaveFallbehindMax;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public String getHaMasterAddress() {
        return haMasterAddress;
    }

    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }

    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }

    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }

    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    /**
     * Enable transient commitLog store pool only if transientStorePoolEnable is true and the FlushDiskType is
     * ASYNC_FLUSH
     * <p>
     * 是否启用 transientStorePoolEnable 机制，只有：开启transientStorePoolEnable & 异步刷盘 & 非从节点
     *
     * @return <tt>true</tt> or <tt>false</tt>
     */
    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable && FlushDiskType.ASYNC_FLUSH == getFlushDiskType()
                && BrokerRole.SLAVE != getBrokerRole();
    }

    public void setTransientStorePoolEnable(final boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(final int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(final int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(final boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(final boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(final int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(final int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }

    public String getPreferredLeaderId() {
        return preferredLeaderId;
    }

    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderId = preferredLeaderId;
    }

    public boolean isEnableBatchPush() {
        return isEnableBatchPush;
    }

    public void setEnableBatchPush(boolean enableBatchPush) {
        isEnableBatchPush = enableBatchPush;
    }

    public boolean isEnableScheduleMessageStats() {
        return enableScheduleMessageStats;
    }

    public void setEnableScheduleMessageStats(boolean enableScheduleMessageStats) {
        this.enableScheduleMessageStats = enableScheduleMessageStats;
    }
}
