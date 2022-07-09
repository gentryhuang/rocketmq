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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 延时队列：
 * 0 todo 延时消息服务不仅充当周期性（不断创建 Timer 的定时任务）扫描延时队列，还充当延时消息配置类（会对延时队列的相关配置进行持久化，如延时队列的消费进度，delayLevelTable 不会）
 * 1 RocketMQ 实现的延时队列只支持特定的延时时段，1s、5s、10s...2h，不能支持任意时间段的延时
 * 2 具体实现：
 * RocketMQ 发送延时消息时先把消息按延迟时间段发送到指定的队列中，RocketMQ 是把每种延迟时间段的消息都存放到同一个队列中，
 * 然后通过一个定时器进行轮询这些队列，查看消息是否到期，如果到期就把这个消息发送到指定 topic 的队列中。
 * 3 优点：
 * 设计简单，把所有相同延迟时间的消息都先放到一个队列中保证了到期时间的有序性，定时扫描，可以保证消息消费的有序性
 * 4 缺点
 * 定时器采用 Timer，Timer 是单线程运行，如果延迟消息数量很大的情况下，可能单线程处理不过来，造成消息到期后也没发送出去的情况
 * 5 改进
 * 可以在每个延迟队列上各采用一个timer，或者使用timer进行扫描，加一个线程池对消息进行处理，这样可以提供效率
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 延时级别到延迟时间的映射
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 延迟级别到队列消费的偏移量的映射
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 存储服务
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 启动扫描延时队列的标志
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 轮询延时队列的定时器 Timer
     */
    private Timer timer;
    /**
     * 消息存储
     */
    private MessageStore writeMessageStore;
    /**
     * 最大延迟级别
     */
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    /**
     * 消息队列id 到 对应的延迟级别
     *
     * @param queueId
     * @return
     */
    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    /**
     * 根据 延迟级别计算消息队列id，queueId = DelayLevel - 1
     *
     * @param delayLevel 延迟级别
     * @return 消息队列编号
     */
    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 计算投递时间 （消费时间）
     *
     * @param delayLevel     延迟级别
     * @param storeTimestamp 存储时间
     * @return 投递时间
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        // 根据延迟级别取出对应的延时时间
        Long time = this.delayLevelTable.get(delayLevel);

        // 计算消息投递时间
        if (time != null) {
            return time + storeTimestamp;
        }

        // 没有对应的延时时间，默认使用 1s
        return storeTimestamp + 1000;
    }

    /**
     * 使用定时器 Timer 启动一个定时任务，把每个延时粒度封装成一个任务，然后加入到 Timer 的任务队列中
     */
    public void start() {

        // 通过AtomicBoolean 来确保有且仅有一次执行start方法
        if (started.compareAndSet(false, true)) {

            // todo  加载延迟消息偏移量文件，默认：$user.home/store/config/delayOffset.json 到内存
            super.load();

            // 创建定时器
            // 内部通过创建并启动一个线程，不断轮询定时器中的任务队列
            // 方法 schedule 和方法 scheduleAtFixedRate ，如果执行任务的时间被延迟了，那么下一次任务的执行时间参考的是上一次任务"结束"时的时间来计算。
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 1 遍历延时级别到延迟时间的映射
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                // 延时级别
                Integer level = entry.getKey();
                // 延时时间
                Long timeDelay = entry.getValue();

                // todo 根据延时级别获取对应的偏移量，即 level 级别对应队列的逻辑偏移量
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                // 每个消费队列对应单独一个定时任务进行轮询，发送 到达投递时间【计划消费时间】 的消息
                if (timeDelay != null) {
                    // 针对 messageDelayLevel 配置，启动对应的 timer 开始 1秒后执行 DeliverDelayedMessageTimerTask,判断消息是否延时到期
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            // 2 定时持久化延时队列的消费进度，每 10s 执行一次
            // 刷新延时队列 topic 对应每个 queueid 的 offset 到本地磁盘（当前主 Broker)。
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     * 加载延时映射表
     *
     * @return
     */
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    /**
     * 将 延迟级别到队列消费偏移量的映射 offsetTable 序列化
     *
     * @param prettyFormat 是否格式化
     * @return
     */
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 维护 delayLevelTable 映射表
     *
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // 获取消息延迟级别字符串配置
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            // 获取每个级别
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                // 计算延时时间
                long delayTimeMillis = tu * num;

                // 维护级别和对应的延时时间
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * 任务，作为调度器 Timer 的任务
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        /**
         * 延时级别，-1 就是对应的 ID
         */
        private final int delayLevel;
        /**
         * 延时级别对应消息队列的逻辑偏移量
         */
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        /**
         * 延时执行 - 被延时调度了
         */
        @Override
        public void run() {
            try {
                // 如果启动了扫描延时队列，则处理延时队列
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                        this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 纠正可投递时间
         * 因为发送级别对应的发送间隔可以调整，如果超过当前间隔，则修正成当前配置，避免后面的消息无法发送
         *
         * @param now              当前时间
         * @param deliverTimestamp 投递时间
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            long result = deliverTimestamp;

            // 重新计算消息投递时间
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);

            // 如果预期的投递时间比重新计算消息投递时间要大，那么以当前时间为准，因为可能延时等级对应的延时时间改变了
            // todo 正常情况下，deliverTimestamp <= maxTimestamp ，如果 deliverTimestamp > maxTimestamp 说明延时等级对应的延时时间改小了，应该立即触发，避免后面的消息无法及时发送；
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        /**
         * 执行遍历
         */
        public void executeOnTimeup() {
            // 1 根据 topic（SCHEDULE_TOPIC_XXXX） 和 queueId 获取消费队列
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                // 2 根据 cq 的消费逻辑偏移量获取消息的索引信息
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        // 2.1 每个扫描任务主要是把队列中所有到期的消息索引都拿出来
                        // 步长是 20,结束条件是： i< bufferCQ.getSize()
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 在 Commitlog 中的偏移量
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            // 消息大小
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            // 预期投递时间
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                            tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }


                            // 2.2 todo 修正可投递时间，因为可能延时等级对应的延时时间会人为改变（改小），这个时候不能以 tagsCode 中的预期消费时间为准
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            // 2.3 下一个逻辑偏移量（第一次的时候 i = 0）
                            // todo 推进了消息队列的偏移量
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            // 2.4 判断延时时间是否到达
                            long countdown = deliverTimestamp - now;

                            // 到达了时间
                            if (countdown <= 0) {

                                // 2.5 根据偏移量和消息大小从 Commitlog 中获取对应的延时消息
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        // 2.6 还原延时消息真实属性
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);

                                        // 这里做防御性编程，防止事务消息使用了延时消息
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }

                                        // todo 2.7 投递真正的消息，这时候没有了延时消息的标志了
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        // 如果发送成功，则继续下一个消息索引的获取与判断是否到期
                                        if (putMessageResult != null
                                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
                                                        putMessageResult.getAppendMessageResult().getWroteBytes());
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());
                                            }
                                            continue;

                                            // 发送消息失败
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                    "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                    msgExt.getTopic(), msgExt.getMsgId());

                                            // 安排下一次任务
                                            ScheduleMessageService.this.timer.schedule(
                                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_PERIOD);

                                            // 更新进度
                                            ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);

                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         */
                                        log.error(
                                                "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                        + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                        + offsetPy + ",sizePy=" + sizePy, e);
                                    }


                                    /*
                                      todo 到期的延时消息重新投递失败，只是打印日志，然后就跳过了改消息(延迟队列消费的偏移量推进了），那岂不是会丢弃？
                                     */
                                }

                                // todo 没有到达预期消费时间，那么就不需要继续往下遍历了，因为同一个队列的到期时间是有序的，前一个没有到期后边的就更没有。留着下次再次从这个偏移量开启遍历与判断是否到达
                            } else {

                                // 安排下一次任务
                                ScheduleMessageService.this.timer.schedule(
                                        new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                        countdown);// 任务触发时间为距离消费时间差

                                // 更新进度
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);

                                return;
                            }
                        } // end of for

                        // 遍历完了拉取的消息索引条目，安排下次任务，继续判断
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                                this.delayLevel, nextOffset), DELAY_FOR_A_WHILE); // 任务触发时间为 100ms
                        // 更新消费进度
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;

                    } finally {
                        bufferCQ.release();
                    }


                } // end of if (bufferCQ != null)
                else {

                    // 消费队列已经被删除部分，跳转到最小的消费进度
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        // todo 下次拉取从最小偏移量开始
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                                + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            // 下次拉取任务，注意消费进度起始位置
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                    failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 构建真正的消息
         *
         * @param msgExt
         * @return
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());

            // todo 重试次数
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            // 清理掉延时标志，不需要再次延时了
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            // 还原真实的 Topic 和 queueId
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }

    }
}
