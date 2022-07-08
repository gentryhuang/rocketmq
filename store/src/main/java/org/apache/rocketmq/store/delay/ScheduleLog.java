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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

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

        // 格式：/Users/huanglibao/store/schedulelog/1657276200000
        String sheduleDir = this.storePath + File.separator + dirMills;

        /**
         * 创建 MappedFile 的队列
         */
        this.mappedFileQueue = new MappedFileQueue(sheduleDir, mappedFileSize, null);
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 恢复消息队列
     */
    public void recover() {
        // 1 获取该消息队列的所有内存映射文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();

        // 2 只从倒数第3个文件开始，这应该是一个经验值
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            // 3 ConsumeQueue 逻辑大小
            int mappedFileSizeLogics = this.mappedFileSize;

            // 倒数第 3 个内存映射文件，或者是第 1 个内存映射文件
            MappedFile mappedFile = mappedFiles.get(index);

            // 内存映射文件对应的 ByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // 该文件的起始物理偏移量，默认从 ConsumeQueue 中存放的第一个条目开始。
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;


            while (true) {

                // 4 循环验证 ConsumeQueue 包含条目的有效性（如果offset大于等于0并且size大于0，则表示是一个有效的条目）
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {

                    // 5 读取一个条目的内容
                    // 5.1 commitlog 中的物理偏移量
                    long offset = byteBuffer.getLong();
                    // 5.2 该条消息的消息总长度
                    int size = byteBuffer.getInt();
                    // 5.3 tag hashcode
                    long tagsCode = byteBuffer.getLong();

                    // 如果 offset 大于 0 & size 大于 0，则表示是一个有效的条目
                    if (offset >= 0 && size > 0) {
                        // 更新 ConsumeQueue 中有效的 mappedFileOffset
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }

                        // 如果发现不正常的条目，则跳出循环
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                // 6 如果该 ConsumeQueue 文件中所有条目全部有效，则继续验证下一个文件，index++）,如果发现条目不合法，后面的文件不需要再检测。
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }

            // 7 processOffset 代表了当前 consuemque 有效的偏移量
            processOffset += mappedFileOffset;
            // 8 设置 flushedWhere，committedWhere 为当前有效的偏移量
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);

            // 9 截断无效的 ConsumeQueue 文件，只保留到 processOffset 位置的有效文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 根据消息存储时间来查找
     *
     * @param timestamp
     * @return
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        // 查找 MappedFile 更新时间 >= timestamp 的 MappedFile
        // 1 根据时间戳定位到物理文件
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);

        // 采用二分查找
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 根据 CommitLog 有效的物理偏移量，清除无效的 ConsumeQueue 数据
     *
     * @param phyOffet CommitLog 最大有效的物理偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;
        this.maxPhysicOffset = phyOffet;
        long maxExtAddr = 1;

        // 从最后一个文件开始遍历
        while (true) {
            // 获取最后一个内存文件
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                // 重置三个指针「清除无效的数据，本质上就是修改指针到有效数据位置」
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                // 逐条遍历记录
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 如果是第一条记录
                    if (0 == i) {

                        // 记录的消息物理偏移量达到了最大有效偏移量，那么说明整个 ConsumeQueue 文件都是无效的，删除物理文件已经内存文件。然后就像前一个文件
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;

                            // 更新三个指针位置
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);

                            // 变更 ConsumeQueue 记录的消息最大物理偏移量
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }

                        // 非第一条记录
                    } else {

                        // 如果当前消息索引有效，那么继续判断。否则，就无需更新三大指针
                        if (offset >= 0 && size > 0) {

                            // 记录的消息物理偏移量达到了最大有效偏移量，那么直接结束。
                            // todo 此时遍历到的文件 MappedFile 是有效的文件，三大指针记录的是有效的位置
                            if (offset >= phyOffet) {
                                return;
                            }

                            // 更新大三指针，以及变更 ConsumeQueue 记录的消息最大物理偏移量
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            // 当前文件遍历完毕，说明此时遍历到的文件 MappedFile 是有效的文件，三大指针记录的是有效的位置
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * 删除 ConsumeQueue 中无效的文件
     *
     * @param offset CommitLog 最小物理偏移量
     * @return
     */
    public int deleteExpiredFile(long offset) {
        // 删除无效的文件
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);

        // 修正当前 ConsumeQueue 最小物理偏移量。
        // 因为前面是根据每个文件最后一个消息条目决定是否删除文件，因此对于剩余的文件中的第一个可能存在无效的条目。
        this.correctMinOffset(offset);

        return cnt;
    }


    /**
     * 修正 ConsumeQueue 最小的物理偏移量
     *
     * @param phyMinOffset CommitLog 最小物理偏移量
     */
    public void correctMinOffset(long phyMinOffset) {
        // 获取第一个 MappedFile 内存文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // 读取该内存文件消息索引条目
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    // 遍历消息索引条目
                    for (int i = 0; i < result.getSize(); i += ScheduleLog.CQ_STORE_UNIT_SIZE) {
                        // 获取消息物理偏移量
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        // 如果当前消息索引条目中记录的物理偏移量 >= CommitLog 最小物理偏移量，那么此时就可以知道当前 ConsumeQueue 最小物理便宜量
                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                    this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    /**
     * 在队列中的最小下标
     *
     * @return
     */
    public long getMinOffsetInQueue() {
        // 计算得到的是 下标
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 根据 commitlog 信息写消息队列信息
     * <p>
     * 为了保证重放消息成功：
     * 1 进行重试，最大 30次
     * 2 写入失败，设置队列不可写，后续再来重放就不可写
     * 3 打印异常日志
     *
     * @param request 由 CommitLog 构建的 request
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        // 最大重试次数
        final int maxRetries = 30;

        // 1 判断 ConsumeQueue 是否可写
        // 当写入失败，会标记 ConsumeQueue 写入异常，不允许继续写入
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();

        // todo 多次循环写，直到成功
        for (int i = 0; i < maxRetries && canWrite; i++) {
            // 消息 tag 哈希码
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                            topic, queueId, request.getCommitLogOffset());
                }
            }

            // 2 调用添加位置信息
            boolean result = this.putMessagePositionInfo(
                    request.getCommitLogOffset(), // 消息在 CommitLog 中的物理偏移量
                    request.getMsgSize(), // 消息大小
                    tagsCode, // 消息的 tag
                    request.getConsumeQueueOffset() // todo 消息在消息队列的逻辑偏移量，为了找到在哪个 ConsumeQueue 的哪个位置开始写
            );

            // 添加成功，使用消息存储时间 作为 存储 check point。
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                        this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;

                // 添加失败，目前基本可以认为是BUG。
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        // todo 写入失败时，标记 ConsumeQueue 写入异常，不允许继续写入
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 添加位置信息，并返回添加是否成功
     * <p>
     * 1 ConsumeQueue 每一个条目都是 20个字节（8个字节commitlog偏移量+4字节消息长度+8字节tag的hashcode）
     * 2 todo 注意，是将内容追加到 ConsumeQueue 的内存映射文件中（只追加，不刷盘），ConsumeQueue 的刷盘方式固定为异步刷盘,
     * 刷盘任务启动是在 {@link DefaultMessageStore#start()}
     *
     * @param offset   消息在 Commitlog 中的物理偏移量，8 字节
     * @param size     消息体大小 4 字节
     * @param tagsCode 消息 tags 的 hashcode ，注意如果是延时消息，则是计划消费时间
     * @param cqOffset 写入 consumequeue 的逻辑偏移量
     * @return
     */
    private boolean putMessagePositionInfo(final long offset,
                                           final int size,
                                           final long tagsCode,
                                           final long cqOffset) {

        // 如果已经重放过，直接返回成功
        // 注意 maxPhysicOffset 值的由来
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        // 1 将一条 ConsueQueue 条目总共20字节，写入 ByteBuffer 缓存区中，即写入位置信息到byteBuffer
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        // 消息在 CommitLog 中的物理偏移量
        this.byteBufferIndex.putLong(offset);
        // 消息长度
        this.byteBufferIndex.putInt(size);
        // tag 哈希吗 （注意如果是延时消息，则是计划消费时）
        this.byteBufferIndex.putLong(tagsCode);

        // 2 todo 根据 cqOffset 逻辑偏移量计算消息索引在 ConsumeQueue 中的物理偏移量开始位置
        // cqOffset=0 -> expectLogicOffset = 0
        // cqOffset=1 -> expectLogicOffset = 20
        // cqOffset=2 -> expectLogicOffset = 40
        // ... todo expectLogicOffset == mappedFile.getWrotePosition() + mappedFile.getFileFromOffset()，因为写入的指针位置就是下次写入数据的位置
        // 比真实的大1
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 3 根据消息在 ConsumeQueue 中的物理偏移量，查找对应的 MappedFile
        // todo 如果找不到，则新建一个 MappedFile，对应的物理文件名称就是 expectLogicOffset
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            // 3 如果文件是新建的，需要先填充前置空白占位
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                // todo  记录最小物理偏移量
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);

                // 填充空格
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }

            // 校验consumeQueue存储位置是否合法。
            if (cqOffset != 0) {
                // todo 获取当前写的物理偏移量
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                // 如果消息预期物理偏移量 < 当前 MappedFile 写入偏移量，说明出现了问题
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                // todo 如果 消息预期物理偏移量 不等于当前 MappedFile 写入偏移量，说明可能出错了
                // todo 因为当前追加的消息索引的物理偏移量必须是上次写入的位置，每个索引长度固定。
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }

            // todo 更新 commitLog 重放消息到当前 ConsumeQueue 的最大位置。
            this.maxPhysicOffset = offset + size;

            // 4 写入消息索引信息到 consumeQueue 文件中，整个过程都是基于 MappedFile 来操作的
            // 即将消息条目缓存区写入到 FileChannel
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }

        return false;
    }

    /**
     * 填充前置空白占位
     *
     * @param mappedFile MappedFile
     * @param untilWhere consumeQueue 存储位置
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        // 写入前置空白占位到byteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        // 循环填空
        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 根据 startIndex 获取消息消费队列条目
     *
     * @param startIndex 逻辑偏移量（针对 ConsumeQueue 文件组），类似索引下标
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;

        // 1 通过 startIndex * 20 得到的在 ConsumeQueue 文件（组）中的物理偏移量
        // todo 因为每个 ConsumeQueue 条目的大小是固定的,所以只需要根据 index*20 则可以定位到物理偏移量 offset 的值
        long offset = startIndex * CQ_STORE_UNIT_SIZE;

        // 2 如果该偏移量 < minLogicOffset ，则返回 null，说明该消息已被删除
        // 如果 >= minLogicOffset，则根据偏移量定位到具体的物理文件
        if (offset >= this.getMinLogicOffset()) {

            // 根据计算得到的物理偏移量，确定其位于哪个 MappedFile
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);

            // 获取对应的数据
            if (mappedFile != null) {

                // todo 通过将该偏移量与物理文件大小取模获取在该文件的逻辑偏移量，从该逻辑偏移量开始读取该文件所有数据
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));

                return result;
            }
        }

        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    /**
     * 根据逻辑偏移量获取下一个消息索引文件
     *
     * @param index
     * @return
     */
    public long rollNextFile(final long index) {
        // 索引文件大小
        int mappedFileSize = this.mappedFileSize;

        // 一个文件可以存储多个索引
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;

        // 下一个文件的起始索引
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
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

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        // 消息索引最大长度 / 20 ，得到在在 ConsumeQueue 中的偏移量
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
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
}
