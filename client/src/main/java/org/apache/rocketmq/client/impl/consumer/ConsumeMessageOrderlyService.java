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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 有序消息处理服务 - 严格顺序消息 （todo 局部还是全局？？？）
 * 说明：Consumer 在严格顺序消费时，通过三把锁保证
 * 1 Broker 消息队列锁（分布式锁）
 * - 集群模式下，Consumer 从 Broker 获得该锁后，才能进行消息拉取、消费
 * - 广播模式下，Consumer 无需该锁
 * 2 Consumer 消息队列锁（本地锁）：Consumer 获得该锁才能操作消息队列
 * 3 Consumer 消息处理队列锁（本地锁）：Consumer 获得该锁才能消费消息队列
 * 小结：
 * ConsumeMessageOrderlyService 在消费的时候，会先获取每一个 ConsumerQueue 的锁，然后从 processQueue 获取消息消费，这也就意味着，对于每一个 ConsumerQueue 的消息来说，消费的逻辑也是顺序的。
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));

    /**
     * 消费者类
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**
     * 消费者对外暴露类
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 监听器
     */
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 消费组
     */
    private final String consumerGroup;
    /**
     * 消息队列锁
     */
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    /**
     * 在消费者启动时会调用该方法，在集群模式下，默认每隔20s执行一次锁定分配给自己的消息消费队列。
     * todo 特别说明：
     * 每个 MQ 客户端，会定时发送 LOCK_BATCH_MQ 请求，并且在本地维护获取到锁的所有队列，即在消息处理队列 ProcessQueue 中使用 locked 和 lastLockTimestamp 进行标记。
     */
    public void start() {
        // 集群消费
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            // 消费方需要不断向 Broker 刷新该锁过期时间，默认配置 20s 刷新一次
            // 默认每隔 20s 执行一次锁定分配给自己的消息消费队列
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 关闭
     * todo 可能出现问题，机器宕机，锁没有释放，需要等待 60s （Broker 端默认60s锁过期）。如果在这期间，消费组内其它实例分配到了该宕机机器，
     * 那么就不能消费消息，造成消费消息延迟
     *
     * @param awaitTerminateMillis
     */
    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);

        // 集群模式下，unlock 所有的队列
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    /**
     * unlock 所有队列
     */
    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageOrderlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提交消息请求
     *
     * @param msgs             消息 ，顺序消息用不到
     * @param processQueue     消息处理队列
     * @param messageQueue     消息队列
     * @param dispathToConsume 派发到消费
     */
    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    /**
     * 周期性锁定当前消费实例分配到的所有 MessageQueue
     */
    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    /**
     * 只有锁住 MessageQueue 当前消费端才能操作消息队列
     *
     * @param mq           消息队列
     * @param processQueue 消息处理队列
     * @param delayMills   延时执行时间
     */
    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
                                         final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                // 锁定 mq 是否成功
                // 第 3 把锁
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                // 锁住 mq ，则立即消费
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);

                    // 没有锁住，3s 后重试
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试锁住执行的 mq
     *
     * @param mq
     * @return
     */
    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    /**
     * 提交延迟消费请求
     *
     * @param processQueue      消息处理队列
     * @param messageQueue      消息队列
     * @param suspendTimeMillis 延迟时间
     */
    private void submitConsumeRequestLater(
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 处理顺序消费结果，并返回是否继续消费
     * 说明：
     * 1 在并发消费场景时，如果消费失败，Consumer 会将消费失败消息发回到 Broker 重试队列，跳过当前消息，等待下次拉取该消息再进行消费。
     * 2 在完全严格顺序消费消费时，如果消费失败，这样做显然不行。也因此，消费失败的消息，会挂起队列一会会，稍后继续消费。即将消息重新放入缓存中，因为它们使用 TreeMap 保存的，
     * 根据消费进度排序
     * 3 不过消费失败的消息一直失败，也不可能一直消费。当超过消费重试上限时，Consumer 会将消费失败超过上限的消息发回到 Broker 死信队列
     *
     * @param msgs           消息
     * @param status         消费结果状态
     * @param context        消费 Context
     * @param consumeRequest 消费请求
     * @return 是否继续消费
     */
    public boolean processConsumeResult(
            final List<MessageExt> msgs,
            final ConsumeOrderlyStatus status,
            final ConsumeOrderlyContext context,
            final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        // 自动提交
        if (context.isAutoCommit()) {
            switch (status) {
                // 考虑到 ROLLBACK 、COMMIT 暂时只使用在 MySQL binlog 场景，官方将这两状态标记为 @Deprecated。
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());

                    // 消费成功
                case SUCCESS:

                    // 提交消息已消费成功到消息处理队列
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    // 统计
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;

                // 等一会继续消费
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    // 统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());

                    // 计算是否暂时挂起（暂停）消费N毫秒，默认：10ms
                    if (checkReconsumeTimes(msgs)) {

                        // 设置消息重新消费，存储消息时会根据消费进度排序
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);

                        // 提交延迟消费请求
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;

                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }

            // 非自动提交
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;

                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;

                // 计算是否暂时挂起（暂停）消费N毫秒，默认：10ms
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        // 设置消息重新消费，存储消息时会根据消费进度排序
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);

                        // 提交延迟消费请求
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        // 消息处理队列未dropped，提交有效消费进度
        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 获取最大重复消费次数
     *
     * @return
     */
    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    /**
     * 计算是否要暂停消费 todo 不明白
     * 不暂停条件：存在消息都超过最大消费次数并且都发回broker成功
     *
     * @param msgs
     * @return
     */
    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {

                // 消息消费次数是否超过最大消费次数，默认上限 16
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));

                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }

                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    /**
     * 发回消息
     * 消息发回 Broker 后，对应的消息队列是死信队列
     *
     * @param msg 消息
     * @return 是否发送成功
     */
    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            // 创建消息，注意 Topic，重试Topic
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);

            // 设置延迟级别
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            // 发送消息
            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    /**
     * 消费请求任务。
     * todo 特别说明：
     * 1 并发消息消费指消费线程池中的线程可以并发对同一个消费队列的消息进行消费
     * 2 顺序消息是指消费线程池中的线程对消息队列只能串行消费。消息消费时必须成功锁定消息队列，在 Broker 端会存储消息队列的锁占用情况。
     */
    class ConsumeRequest implements Runnable {
        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            // 如果消费处理队列废弃，什么都不干
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            // 根据消息队列获得消息队列的 锁对象
            // 顺序消息一个消息消费队列同一时刻只会被一个消费线程池处理
            // 1 第一把锁
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);

            // JVM 加锁（这里加 JVM 锁就够了，因为每个消费方都有自己的队列）
            synchronized (objLock) {

                // todo (广播模式) 或者 (集群模式 && 消息处理队列锁有效)
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        // 2 第二把锁
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {

                    // 记录开始时间
                    final long beginTime = System.currentTimeMillis();

                    // 循环
                    // continueConsume 是否继续消费
                    for (boolean continueConsume = true; continueConsume; ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        // todo 消息队列锁未锁定，提交延迟获得锁并消费请求
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);

                            // 提交延迟获得锁，后续会再次执行该进入到 run() 方法流程
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // 消息队列锁已经过期，提交延迟获得锁并消费请求
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);

                            // 提交延迟获得锁，后续会再次执行该进入到 run() 方法流程
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // 当前周期消费时间超过连续时长，默认：60s，提交延迟消费请求。
                        // 默认情况下，每消费1分钟休息10ms
                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        // 获取批量方式消费的消息大小
                        final int consumeBatchSize =
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        // 获取指定大小的消息
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);

                        // todo 处理消息的 Topic，还原 Topic,针对重试 Topic
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

                        // 有消息
                        if (!msgs.isEmpty()) {
                            // 创建上下文
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            // Hook
                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                        .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {

                                // 锁定队列消费锁
                                this.processQueue.getConsumeLock().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                    break;
                                }

                                // todo 消费方消费消息
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                                hasException = true;
                            } finally {
                                // 释放队列消费锁
                                this.processQueue.getConsumeLock().unlock();
                            }

                            if (null == status
                                    || ConsumeOrderlyStatus.ROLLBACK == status
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                            }


                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            // 顺序消费消息结果
                            if (null == status) {
                                // 有异常
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;

                                    // 返回 null
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }

                                // 消费超时
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;

                                // 消费失败，挂起消费队列一会会，稍后继续消费。
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;

                                // 消费成功但不提交
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            // Hook
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                        .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                    .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

                            // todo 处理消费结果
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);

                            // 没有消息，则
                        } else {
                            continueConsume = false;
                        }
                    }

                    // 没有锁定 ConsumeQueue
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    // 延迟重试
                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
