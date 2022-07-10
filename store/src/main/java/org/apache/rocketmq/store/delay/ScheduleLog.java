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
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * 必要性说明：
 * 1 RocketMQ 是通过订阅Topic来消费消息的，但是因为 CommitLog 是不区分topic存储消息的
 * 2 消费者通过遍历commitlog去消费消息 那么效率就非常低下了，所以设计了 ConsumeQueue ，作为 CommitLog 对应的索引文件
 * <p>
 * 前置说明：
 * 1 ConsumeQueue : MappedFileQueue : MappedFile = 1 : 1 : N
 * 2 MappedFile : 00000000000000000000等文件
 * 3 MappedFileQueue:
 * - 对 MappedFile 进行封装成文件队列，对上层提供可无限使用的文件容量。
 * - 每个 MappedFile 统一文件大小
 * - 文件命名方式：fileName[n] = fileName[n - 1] + mappedFileSize
 * 4 ConsumeQueue 存储在 MappedFile 的内容必须大小是 20B( ConsumeQueue.CQ_STORE_UNIT_SIZE )
 * <p>
 * <p>
 * 消息消费队列，引入的目的主要是提高消息消费的性能，由于RocketMQ是基于主题topic的订阅模式，消息消费是针对主题进行的，如果要遍历commitlog文件中根据topic检索消息是非常低效的。
 * 特别说明：
 * 1 运行过程中，消息发送到commitlog文件后，会同步将消息转发到消息队列（ConsumeQueue）
 * 2 broker 启动时，检测 commitlog 文件与 consumequeue、index 文件中信息是否一致，如果不一致，需要根据 commitlog 文件重新恢复 consumequeue 文件和 index 文件。
 */
public class ScheduleLog {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

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
     * @see ScheduleLog#recover()  通过 ConsumeQueue 计算得来的，后续随着重放消息进行更新
     */
    private long maxPhysicOffset = -1;
    /**
     * 当前 ConsumeQueue 最小物理偏移量
     */
    private volatile long minLogicOffset = 0;

    private ConsumeQueueExt consumeQueueExt = null;

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

        // 格式：/Users/huanglibao/store/schedulelog/1657276200000
        this.scheduleDir = this.storePath + File.separator + dirMills;
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

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }


    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
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
}
