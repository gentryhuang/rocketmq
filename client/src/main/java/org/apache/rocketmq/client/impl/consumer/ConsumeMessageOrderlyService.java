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
 * 有序消息处理服务 - 严格顺序消息 （todo 注意是局部的，因为只解决 Topic 下一个 MessageQueue 的顺序，无法解决 Topic 级别的有序，除非 Topic 只有一个 MessageQueue）
 * 说明：Consumer 在严格顺序消费时，通过三把锁保证
 * 1 Broker 消息队列锁（分布式锁）
 * - 集群模式下，Consumer 从 Broker 获得该锁后，才能进行消息拉取、消费；
 * - 广播模式下，Consumer 无需分布式锁，因为每个消费者都有相同的队列，只需要保证同一个消费队列同一时刻只能被一个线程消费即可；
 * 2 Consumer 消息队列锁（本地锁）：Consumer 获得该锁才能操作消息队列
 * 3 Consumer 消息处理队列消费锁（本地锁）：Consumer 获得该锁才能消费消息队列
 * 小结：
 * 1 ConsumeMessageOrderlyService 在消费的时候，会先获取每一个 ConsumerQueue 的锁，然后从 processQueue 获取消息消费，这也就意味着，对于每一个 ConsumerQueue 的消息来说，消费的逻辑也是顺序的。
 * 2 todo 对消息队列做什么事情之前，先申请该消息队列的锁。无论是创建消息队列拉取任务「分布式锁」、拉取消息「分布式锁」、消息消费「分布式锁、消息队列锁、消息处理队列消费锁」，无不如此。
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();

    /**
     * 消费任务一次运行的最大时间，默认为 60s
     */
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));

    /**
     * 消息消费者实现类
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**
     * 消息消费者（消费者对外暴露类）
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 顺序消息消费监听器
     */
    private final MessageListenerOrderly messageListener;
    /**
     * 消息消费任务
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消息消费线程池
     */
    private final ThreadPoolExecutor consumeExecutor;
    /**
     * 消费组
     */
    private final String consumerGroup;
    /**
     * 消息消费队列锁对象
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

        // 消息消费线程池
        // todo 由于使用的是无界队列，因此线程池中最大线程数是无效的，故有效线程数是核心线程 consumeThreadMin
        this.consumeExecutor = new ThreadPoolExecutor(
                // 核心线程数
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                // 最大线程数 (该参数无效，因为使用的任务队列是 LinkedBlockingQueue)
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                // 任务队列
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        // 定时线程池，用于延迟提交消费请求任务
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    /**
     * 在消费者启动时会调用该方法，在集群模式下，默认每隔20s执行一次锁定分配给自己的消息消费队列。
     * todo 特别说明：
     * 1 每个 MQ 客户端，会定时发送 LOCK_BATCH_MQ 请求，并且在本地维护获取到锁的所有队列，即在消息处理队列 ProcessQueue 中使用 locked 和 lastLockTimestamp 进行标记。
     * 2 Broker 端锁的有效时间为 60s
     */
    public void start() {
        // 集群消费
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            // 消费方需要不断向 Broker 刷新该锁过期时间，默认配置 20s 刷新一次
            // todo 默认每隔 20s 执行一次锁定分配给自己的消息消费队列
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // todo 锁定 broker 上当前消费者分配到的队列（可能对应多个 Broker )
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
     * @param msgs             消息 ，todo 顺序消息用不到,而是从消息队列中顺序去获取
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
            // 创建消费消息任务
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            // 提交到线程池
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
                // 分布式锁，向 Broker 发起锁定 mq 的请求
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
            // 默认 1000ms
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        // 延迟一段时间再提交到消息线程池中
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
     * 2 在完全严格顺序消费消息时，如果消费失败，这样做显然不行。也因此，消费失败的消息，会挂起队列一会会，稍后继续消费。todo 即将消息重新放入缓存中，因为它们使用 TreeMap 保存的，根据消费进度排序
     * 3 不过消费失败的消息一直失败，也不可能一直消费。当超过消费重试上限时，Consumer 会将消费失败超过上限的消息发回到 Broker ，放入死信队列中（todo 以发送普通消息的形式发送达到最大重试次数的重试消息）
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

        // 执行重试时，将不更新消息消费进度
        long commitOffset = -1L;

        // 自动提交，默认就是自动提交
        if (context.isAutoCommit()) {
            switch (status) {
                // 考虑到 ROLLBACK 、COMMIT 暂时只使用在 MySQL binlog 场景，官方将这两状态标记为 @Deprecated。
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());

                    // 消费成功
                case SUCCESS:

                    // 提交消息已消费成功到消息处理队列，即将临时消息清理掉-这些消息被消费了，并返回下次应该从哪里消费（从 consumingMsgOrderlyTreeMap 中取最大的消息偏移量 offset，然后 + 1)
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    // 统计
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;

                // 等一会继续消费
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    // 统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());

                    // todo 检查消息的重试次数，计算是否暂时挂起（暂停）消费N毫秒，默认：1000ms，然后继续消费。没有达到最大次数，就属于无效消费，不更新消费进度。
                    if (checkReconsumeTimes(msgs)) {
                        /*------------- 消息消费重试 ---------*/

                        // todo 将该批消息重新放入到 ProcessQueue 的 msgTreeMap，然后清除consumingMsgOrderlyTreeMap，默认延迟1s再加入到消费队列中，并结束此次消息消费。
                        // todo 注意，存储消息时会根据消费进度排序
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);

                        // 提交延迟消费请求
                        // todo 如果执行消息重试，因为消息消费进度并未向前推进，故本地视为无效消费，将不更新消息消费进度
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;

                        // 消息重试次数 >= 允许的最大重试次数，则将消息发送到 Broker，该消息最终会放入 DLQ （死信队列），RocketMQ 不会再次消费，需要人工干预。
                        // todo 这种情况，提交该批消息，表示消息消费成功
                    } else {

                        // 返回待保存的消息消费进度，即将临时消息清理掉-这些消息被消费了，并返回下次应该从哪里消费
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
                    // 提交消息已消费成功到消息处理队列
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    // 提交延迟消费请求
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

                    // todo 检查消息的重试次数，计算是否暂时挂起（暂停）消费N毫秒，默认：1000ms，然后继续消费
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

        // todo 消息处理队列未dropped，提交有效消费进度
        // 逻辑偏移量
        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 获取最大重复消费次数，默认为 -1 ，也就是 16 次
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
     * 检查消息重试次数，计算是否要暂停消费（过一段时间继续消费）
     * 不再消费条件：
     * - 存在消息超过最大消费次数并且发回broker成功
     *
     * @param msgs
     * @return
     */
    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        // 是否暂停消费
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {

                // 消息消费次数是否超过最大消费次数，默认上限 16
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    // 设置重试次数
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));

                    // todo  达到重试上限，将消息发回 Broker 后放入死信队列。因为完全严格顺序消费时，不能发回到 Broker 进行重试。
                    if (!sendMessageBack(msg)) {
                        // 发送失败，则暂停消费
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }

                    // 没有达到最大消费次数，累加重复消费次数，并暂停消费
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
            // todo 创建消息，注意 Topic，重试 Topic。在处理重试消息过程中会判断是否达到最大重试次数，达到会放到死信队列中
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            // 消息id 不变
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            // 重试消息对应的原 主题
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            // 重试次数
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            // 最大重试次数
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);

            // 设置延迟级别
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            // 发送消息
            // todo 以普通消息形式发送重试消息
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
     * 3 有序消费服务中的消费请求任务，不是直接处理拉取到的消息集合，而是从消息处理队列中的消息快照中取消息并消费。
     * 4 并发消费服务中是一次处理完消息集合，而有序消费服务则超时处理消息，默认一个线程消费 1min 就歇息，不再继续消费，把剩余的消息再次提交到线程池中。
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
            // 意味着一个消费者内消费线程池中的线程并发度是消息消费队列级别，同一个消费队列在同一时刻只会被一个线程消费，其他线程排队消费。
            // todo 1 第一把锁
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);

            // JVM 加锁（这里加 JVM 锁就够了，因为每个消费方都有自己的队列）
            synchronized (objLock) {

                // todo (广播模式-不需要分布式锁) 或者 (集群模式 && 消息处理队列锁有效)；广播模式的话直接进入消费，无需使用分布式锁锁定消费队列，因为相互直接无竞争
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        // 2 todo 第二把锁，其实就是 Broker 分布式锁的体现
                        // 防御性编程，再次判断是否还持有队列的分布式锁
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {

                    // 记录开始时间
                    final long beginTime = System.currentTimeMillis();

                    // 循环
                    // continueConsume 是否继续消费
                    for (boolean continueConsume = true; continueConsume; ) {

                        // todo 每次继续消费前判断消息处理队列是否已经被废弃
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        // 集群消费 -  todo 消息队列锁未锁定，提交延迟获得锁并消费请求
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);

                            // 提交延迟获得锁，后续获得锁会再次执行该进入到 run() 方法流程；即获得分布式锁会重新提交消费任务。
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // 集群消费 - 消息队列锁已经过期，提交延迟获得锁并消费请求
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);

                            // 提交延迟获得锁，后续会再次执行该进入到 run() 方法流程
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // todo 当前周期消费时间超过连续时长，默认：60s，提交延迟消费请求任务。每一个ConsumeRequest消费任务不是以消费消息条数来计算，而是根据消费时间（超过消费时间，把剩余消息交给其它线程消费），
                        //  默认当消费时长大于MAX_TIME_CONSUME_CONTINUOUSLY，默认60s后，本次消费任务结束，由消费组内其他线程继续消费。
                        // 默认情况下，每消费1分钟休息10ms
                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        // 获取批量方式消费的消息大小
                        final int consumeBatchSize =
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        // 从消息处理队列 ProcessQueue 中的消息快照中获取指定条数的消息
                        // todo 注意，从 ProcessQueue 中取出的消息，会临时存储在 ProcessQueue 的 consumingMsgOrderlyTreeMap 属性中
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

                            // 消费开始时间
                            long beginTimestamp = System.currentTimeMillis();
                            // 消费返回类型
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            // 消费是否有异常
                            boolean hasException = false;

                            try {

                                // 3 todo 第三把锁，消息处理队列 ProcessQueue 消费锁 - AQS
                                // todo 说明：
                                //  1) 相比 MessageQueue 锁，其粒度较小
                                //  2) 在释放 MessageQueue 分布式锁时，可以根据消费锁快速判断是否有线程还在消费消息，只有确定没有线程消费消息时才能尝试释放 MessageQueue 的分布式锁，否则可能导致两个消费者消费同一个队列消息，
                                //     同时，在释放分布锁时，这里也不能获取到消费锁，会阻塞，直到释放了分布式锁。释放了分布式后，对应消息处理队列会被作废、丢弃。
                                this.processQueue.getConsumeLock().lock();

                                // 消息处理队列被丢弃，则直接结束本次消息消费。
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                    break;
                                }

                                // todo 消费方消费消息，并返回消费结果
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                                hasException = true;
                            } finally {
                                // todo 释放队列消费锁
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


                            // 消费时间
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

                            // 消费返回 null
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

                            // todo 处理消费结果，返回是否继续消费（消费失败 & 没有达到最大重新消费次数，就会延时提交重新消费任务，就会返回 false，不再继续消费）
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);

                            // 消息被消费完了，也结束本次消费
                        } else {
                            continueConsume = false;
                        }
                    }

                    // todo 没有锁定 ConsumeQueue，则只有等待获取到锁才能尝试消费
                } else {

                    // 判断是否被废弃，废弃直接结束消费
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    // 尝试获取 ConsumeQueue 的分布锁并再次提交消费任务
                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }
    }

}
