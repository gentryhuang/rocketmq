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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.delay.config.ScheduleMessageStoreConfig;
import org.apache.rocketmq.store.delay.tool.DirConfigHelper;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消息主体以及元数据的存储主体，存储Producer端写入的消息主体内容,消息内容不是定长的。
 * <p>
 * 前置说明：
 * 1 CommitLog : MappedFileQueue : MappedFile = 1 : 1 : N
 * 2 为什么 CommitLog 文件要设计成固定大小的长度呢？为了使用 内存映射机制
 * <p>
 * 说明：
 * 1 单个 commitlog 文件默认大小为 1G，由多个 commitlog 文件来存储所有的消息。commitlog 文件的命令使用存储在该文件中的第一条消息在整个 commitlog 文件组中的偏移量来命名，
 * 即该文件在整个 commitlog 文件组中的偏移量来命名的，举例：
 * 例如一个 commitlog 文件，1024 个字节
 * 第一个文件： 00000000000000000000 ，起始偏移量为 0
 * 第二个文件： 00000000001073741824 ，起始偏移量为 1073741824
 * 2 MappedFile 封装了一个个的 CommitLog 文件，而 MappedFileQueue 就是封装了一个逻辑的 commitlog 文件，这个 MappedFile 队列中的元素从小到大排列
 * 3 MappedFile 又封装了 JDK 中的 MappedByteBuffer
 * 4 同步刷盘每次发送消息，消息都直接存储在 MappedFile 的 MappdByteBuffer，然后直接调用 force() 方法刷写到磁盘，
 * 等到 force 刷盘成功后，再返回给调用方（GroupCommitRequest#waitForFlush）就是其同步调用的实现。
 * 5 异步刷盘：
 * 分为两种情况，是否开启堆外内存缓存池，具体配置参数：MessageStoreConfig#transientStorePoolEnable。
 * 5.1 transientStorePoolEnable = true
 * 消息在追加时，先放入到 writeBuffer 中，然后定时 commit 到 FileChannel,然后定时flush。
 * 5.2 transientStorePoolEnable=false（默认）
 * 消息追加时，直接存入 MappedByteBuffer(pageCache) 中，然后定时 flush
 *
 * <p>
 * Store all metadata downtime for recovery, data protection reliability
 */
public class ScheduleLogManager {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * 消息存储核心对象
     */
    protected final ScheduleMessageStore defaultMessageStore;
    /**
     * 追加消息函数
     */
    private final AppendMessageCallback appendMessageCallback;
    /**
     * 提交偏移量
     */
    protected volatile long confirmOffset = -1L;

    /**
     * 上锁的开始时间
     */
    private volatile long beginTimeInLock = 0;

    /**
     * 写入消息需要申请的锁
     */
    protected final PutMessageLock putMessageLock;

    /**
     * 延时时间分区对应的延时消息文件
     */
    protected final HashMap<Long, ScheduleLog> scheduleLogTable = new HashMap<>(1024);
    /**
     * 分区延时消息对应的已经投递到 CommitLog 的最新的延时时间 - 持久化
     * <p>
     * 定时任务持久化
     */
    private final ConcurrentMap<Long/*分区起始时间 */, Long/* delayTimeSecond */> scheduleDelayTimeTable;

    /**
     * 已经加载到内存时间轮的物理消息偏移量 - 不持久化
     * - 尽可能避免重复加载到时间轮
     * - 作为拉取 ScheduleLog 的物理偏移量
     */
    private final ConcurrentMap<Long/*分区起始时间 */, Long/* offset */> scheduleLogMemoryIndexTable;

    public ScheduleLogManager(final ScheduleMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        // 追加消息的回调函数
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        scheduleDelayTimeTable = new ConcurrentHashMap<>(1024);
        scheduleLogMemoryIndexTable = new ConcurrentHashMap<>(1024);
    }

    public ScheduleMessageStore getScheduleMessageStore() {
        return defaultMessageStore;
    }

    public HashMap<Long, ScheduleLog> getScheduleLogTable() {
        return scheduleLogTable;
    }

    public ConcurrentMap<Long, Long> getScheduleDelayTimeTable() {
        return scheduleDelayTimeTable;
    }

    public ConcurrentMap<Long, Long> getScheduleLogMemoryIndexTable() {
        return scheduleLogMemoryIndexTable;
    }

    /**
     * 删除过期文件
     *
     * @param expiredTime         过期时间戳
     * @param deleteFilesInterval 删除文件间的间隔时间
     * @param intervalForcibly    第一次拒绝删除之后能保留文件的最大时间
     * @param cleanImmediately    是否立即删除
     * @return
     */
    public int deleteExpiredFile(
            final MappedFileQueue mappedFileQueue,
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately) {
        return mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(MappedFileQueue mappedFileQueue, final long offset) {
        // 如果 offset == 0 ，没有找到就返回第一个就可以
        return this.getData(mappedFileQueue, offset, offset == 0);
    }

    /**
     * 根据偏移量从 MappedFile 中获取数据
     *
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(MappedFileQueue mappedFileQueue, final long offset, final boolean returnFirstOnNotFound) {
        // 获取 MappedFile 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // 根据给定的 偏移量获取该偏移量所在的物理 MappedFile
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);

        // todo 读取偏移量相关数据，怎么读取的？
        if (mappedFile != null) {
            // 计算 offset 在 MappedFile 中的偏移量
            int pos = (int) (offset % mappedFileSize);

            // 获取当前 MappedFile 从传入偏移量到写入范围内容数据
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }


    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }


    /**
     * 根据消息体、主题、和属性的长度，结合消息存储格式，计算消息的总长度
     *
     * @param sysFlag
     * @param bodyLength
     * @param topicLength
     * @param propertiesLength
     * @return
     */
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


    /**
     * 异步存储 Message
     *
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {

        // 设置消息存储时间
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 获取消息要发送的 Topic 和 Queue
        String topic = msg.getTopic();
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        // todo 根据延时时间获取 ScheduleLog 对应映射文件
        String property = msg.getProperty(DirConfigHelper.DELAY_TIME);
        Long dirNameByMills = DirConfigHelper.getDirNameByMills(Long.parseLong(property));
        ScheduleLog scheduleLog = scheduleLogTable.get(dirNameByMills);

        // 没有则创建对应时间分区的 ScheduleLog
        if (scheduleLog == null) {
            scheduleLog = createScheduleLog(dirNameByMills);
        }

        // 获取当前时间分区中的映射文件组
        MappedFileQueue mappedFileQueue = scheduleLog.getMappedFileQueue();
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();

        // 上锁，独占锁或自旋转锁
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;
            msg.setStoreTimestamp(beginLockTimestamp);
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            // 追加消息到 ScheduleLog 中
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);

            switch (result.getStatus()) {
                case PUT_OK:
                    break;

                // 创建一个新文件继续写
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        // 创建追加消息的结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // todo 提交刷盘请求 - 异步
        CompletableFuture<PutMessageStatus> flushResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);

        // todo 提交复制请求 - 参考 CommitLog
        CompletableFuture<PutMessageStatus> replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);


        // 将刷盘、复制当成一个联合任务执行
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            // 刷盘任务
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }

            // 复制任务
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} tags: {} client address: {}",
                            msg.getTopic(), msg.getTags(), msg.getBornHostNameString());
                }
            }
            return putMessageResult;
        });
    }

    /**
     * 创建当前延时时间对应的分区 ScheduleLog
     *
     * @param dirNameByMills ScheduleLog 对应的时间分区
     * @return
     */
    private ScheduleLog createScheduleLog(Long dirNameByMills) {
        ScheduleMessageStoreConfig messageStoreConfig = new ScheduleMessageStoreConfig();

        // 创建类似 ConsumeQueue
        ScheduleLog scheduleLog = new ScheduleLog(
                dirNameByMills,
                messageStoreConfig.getStorePathScheduleLog(), // 文件路径
                messageStoreConfig.getMaxMessageSize(), // 一个文件的大小
                defaultMessageStore // 存储
        );

        scheduleLogTable.put(dirNameByMills, scheduleLog);
        return scheduleLog;
    }


    /**
     * 获取当前 CommitLog 的最小偏移量（非某个 CommitLog 文件）
     *
     * @return
     */
    public long getMinOffset(MappedFileQueue mappedFileQueue) {
        // 获取 commitlog 目录下的第一个文件
        MappedFile mappedFile = mappedFileQueue.getFirstMappedFile();

        // 如果第一个文件可用，则返回该文件的起始偏移量，否则返回下一个文件的起始偏移量
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
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
    public SelectMappedBufferResult getMessage(MappedFileQueue mappedFileQueue, final long offset, final int size) {
        // 1 获取 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // 2 根据偏移量 offset ，找到偏移量所在的 Commitlog 文件
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);

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
     * 根据 offset ，返回 offset 所在 CommitLog 文件的下一个 CommitLog 文件的起始偏移量
     *
     * @param offset 物理偏移量
     * @return
     */
    public long rollNextFile(final long offset) {
        // 获取每个 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // （offset + 文件大小 ） -> 跳到下一个文件
        // 减去多余的 offset ，就可以得到起始偏移量
        return offset + mappedFileSize - offset % mappedFileSize;
    }


    /**
     * 追加消息回调
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        /**
         * 存储在内存中的消息编号字节Buffer
         */
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgIdV6Memory;
        /**
         * 存储在内存中的消息字节Buffer
         */
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;

        /**
         * 消息最大长度
         */
        // The maximum length of the message
        private final int maxMessageSize;

        /**
         * 计算方式：topic + "-" + queueId
         */
        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 消息写入字节缓冲区
         *
         * @param fileFromOffset 该文件在整个文件组中的偏移量，文件起始偏移量
         * @param byteBuffer     NIO 字节容器
         * @param maxBlank       最大可写字节数，文件大小-写入位置
         * @param msgInner       消息内部封装实体
         * @return
         */
        @Override
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            // todo 物理偏移量，即相对整个 CommitLog 文件组，消息已经写到哪里了，目前写到了 wroteOffset。
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 消息类型
            int sysflag = msgInner.getSysFlag();

            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            this.resetByteBuffer(storeHostHolder, storeHostLength);

            // todo 这里应该叫 offsetMsgId ，该ID中包含很多信息
            String msgId;

            // 2 创建全局唯一的 msgId，底层存储由16个字节表示
            // 格式：4字节当前 Broker IP + 4字节当前 Broker 端口号 + 8字节消息物理偏移量
            // todo 在 RocketMQ中，只需要提供 offsetMsgId，可用不必知道该消息所属的topic信息即可查询该条消息的内容。
            if ((sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            } else {
                msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            }

            // 3 todo 根据 topic-queueId 获取该队列的偏移地址(待写入的地址)，如果没有，新增一个键值对，当前偏移量为 0
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());

            // 根据 key 获取消费队列的逻辑偏移量 offset
            // 对于 ScheduleLog 没用
            Long queueOffset = 0L;


            // 4 todo 对事务消息需要单独特殊的处理(PREPARE,ROLLBACK类型的消息，不进入Consume队列)
            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * 5 对消息进行序列化，并获取序列化后消息的长度
             * Serialize message
             */

            // 5.1 消息的附加属性长度不能超过 65536 个字节
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            // 5.2 消息的 topic 即长度
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            // 5.3 消息体长度
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 6 计算消息存储长度
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // 7 如果消息长度超过配置的消息总长度，则返回 MESSAGE_SIZE_EXCEEDED
            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                ScheduleLogManager.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }


            // 8 如果 MapperFile 剩余空间不足时，写入 BLANK 占位，，返回结果 END_OF_FILE ，后续会新创建 CommitLog 文件来存储该消息
            // todo 从这里可以看出，每个 CommitLog 文件最少空闲 8 个字节。高 4 字节存储当前文件的剩余空间，低 4 字节存储魔数 CommitLog.BLANK_MAGIC_CODE
            // 也就是文件可用的空间放不下一个消息，为了区分，在每一个commitlog 文件的最后会写入8个字节，表示文件的结束。
            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE - 消息总长度，4 个字节
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE - 魔数，4 个字节
                this.msgStoreItemMemory.putInt(ScheduleLogManager.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = ScheduleLogManager.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);

                // 返回 END_OF_FILE
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, ScheduleLogManager.this.defaultMessageStore.now() - beginTimeMills);
            }


            /**-------- 9 将消息根据 消息的结构 ，顺序写入 MapperFile 中（内存中） --------*/
            // Initialization of storage space
            // todo 重置 msgStoreItemMemory ，指定只能写 msgLen 长度
            this.resetByteBuffer(msgStoreItemMemory, msgLen);

            // 1 TOTALSIZE 消息条目总长度，4字节。注意：CommitLog 条目是不定长的，每个条目的长度存储在前 4 个字节
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE  MAGICCODE 魔数，4 字节
            this.msgStoreItemMemory.putInt(ScheduleLogManager.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC 消息体的 crc 校验码，4字节
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID 消息消费队列ID，4字节
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG FLAG 消息标记，RocketMQ 对其不做处理，供应用程序使用，默认4字节
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET - 消息队列逻辑偏移量，8字节
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET  消息在 CommitLog 文件中的物理偏移量，8字节
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG 消息系统标记，例如是否压缩、是否是事务消息等
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP 消息生产者调用消息发送 API 的时间戳，8字节
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST 消息发送者 IP、端口号，8字节
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP 消息存储时间戳，8字节
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS Broker 服务器 IP + 端口号，8字节
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES  消息重试次数，4字节
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset  8字节
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY 消息体长度和具体的消息内容 4字节
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                this.msgStoreItemMemory.put(msgInner.getBody());
            }
            // 16 TOPIC  TOPIC 主题存储长度和内容，1字节
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES 消息属性长度，2字节
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                this.msgStoreItemMemory.put(propertiesData);
            }

            // 将消息存储到 ByteBuffer 。
            // todo 注意，这里只是将消息存储在 MappedFile 对应的内存映射 Buffer 中，并没有写入磁盘
            final long beginTimeMills = ScheduleLogManager.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            // 10 创建 AppendMessageResult 对象，返回，其状态为 PUT_OK，即写入 MappedFile 成功
            AppendMessageResult result = new AppendMessageResult(
                    AppendMessageStatus.PUT_OK,
                    wroteOffset,
                    msgLen,
                    msgId, // offsetMsgId
                    msgInner.getStoreTimestamp(),
                    queueOffset,
                    ScheduleLogManager.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;

                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    break;
                default:
                    break;
            }
            return result;
        }

        @Override
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = 0L;
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = ScheduleLogManager.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            this.resetByteBuffer(storeHostHolder, storeHostLength);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(storeHostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    ScheduleLogManager.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(ScheduleLogManager.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, ScheduleLogManager.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId;
                if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                    msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                } else {
                    msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                }

                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, ScheduleLogManager.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }
}
