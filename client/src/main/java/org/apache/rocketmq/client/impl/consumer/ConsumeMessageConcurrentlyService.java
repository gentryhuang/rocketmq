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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 监听器
     */
    private final MessageListenerConcurrently messageListener;

    /**
     * 消费线程池队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消费线程池，消费任务 ConsumeRequest 提交到该线程池执行
     */
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 消费组
     */
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    /**
     * 消费方启动时会创建
     *
     * @param defaultMQPushConsumerImpl
     * @param messageListener
     */
    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                             MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        // 消费者设置的监听器
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        /**
         * 消费线程池
         */
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /**
     * 启动 定时清理过期消息，默认周期为 15min
     */
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                // 清理过期的消息
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
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
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
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
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提交立即消费请求
     *
     * @param msgs              消息列表
     * @param processQueue      消息处理队列
     * @param messageQueue      消息队列
     * @param dispatchToConsume
     */
    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispatchToConsume) {

        // 批量消息的数量
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

        // 提交消息数小于等于批量消息数，直接提交消费请求
        if (msgs.size() <= consumeBatchSize) {

            // 创建消费消息任务，将消息封装到里面
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {

                // 提交消费消息任务到线程池
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }

            // 提交消息数大于批量消息数，进行分拆成多个请求
        } else {
            for (int total = 0; total < msgs.size(); ) {
                // 计算当前拆分请求包含的消息
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                // 提交拆分消费请求
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    // 提交请求被拒绝，则将当前拆分消息 + 剩余消息提交延迟消费请求，结束拆分循环。
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    /**
     * 清理过期消息
     */
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
                this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();

            // 消息处理队列
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 处理消费结果
     * todo 特别说明：
     * 1 RocketMQ为了保证高可用，如果Consumer消费消息失败（只要 回调函数没有返回 CONSUME_SUCCESS）就需要重新让消费者消费该条消息。
     * 2 消息重试的策略是什么？Broker 端采用延迟消息的方式，供Consumer再次消费。
     * 3 更新消费进度
     * 3.1 LocalFileOffsetStore模式下，将offset信息转化成json保存到本地文件中；RemoteBrokerOffsetStore则offsetTable将需要提交的MessageQueue的offset信息通过MQClientAPIImpl提供的接口updateConsumerOffsetOneway()提交到broker进行持久化存储。
     * 3.2 由于是先消费再更新offset，因此存在消费完成后更新offset失败，但这种情况出现的概率比较低，更新offset只是写到缓存中，是一个简单的内存操作，出错的可能性较低。
     * 3.3 由于offset先存到内存中，再由定时任务每隔10s提交一次，存在丢失的风险，比如当前client宕机等，从而导致更新后的offset没有提交到broker，再次负载时会重复消费。因此consumer的消费业务逻辑需要保证幂等性。
     *
     * @param status         消费结果状态
     * @param context        上下文
     * @param consumeRequest 消费请求
     */
    public void processConsumeResult(
            final ConsumeConcurrentlyStatus status,
            final ConsumeConcurrentlyContext context,
            final ConsumeRequest consumeRequest) {

        // 确认 index
        int ackIndex = context.getAckIndex();

        // 消息为空，直接返回
        if (consumeRequest.getMsgs().isEmpty())
            return;

        // 计算 ackIndex 值。consumeRequest.msgs[0 - ackIndex]为消费成功，需要进行 ack 确认。
        switch (status) {
            // 消费成功
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }

                // 统计成功/失败数量
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;

            // 消费延迟
            case RECONSUME_LATER:
                ackIndex = -1;
                // 统计成功/失败数量
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }


        // 针对不同的消息模式做不同的处理
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            // 广播模式，无论是否消费失败，不发回消息到 Broker 进行重试，只打印日志
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;

            // 集群模式，消费失败的消息发回到 Broker
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());

                // RECONSUME_LATER 时，ackIndex 为-1，执行循环。CONSUME_SUCCESS 时不会执行循环
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);

                    // todo 回退Msg到Broker，稍后重新消费
                    boolean result = this.sendMessageBack(msg, context);

                    // 发回消息到 Broker 失败，则加入到 msgBackFiled 集合中
                    // todo 注意，可能实际发到了 Broker ，但是 Consumer 以为发送失败了
                    if (!result) {
                        // 记录消息重消费次数
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                // 发回 Broker 失败的消息即重试失败，直接提交延迟（5s）重新消费
                // todo 如果发回 Broker 成功，结果因为例如网络异常，导致 Consumer以为发回失败，判定消费发回失败，会导致消息重复消费，
                //  因此，消息消费要尽最大可能性实现幂等性。
                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);
                    // 提交延迟重新请求
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        // 消息完成消费（消费成功 和 消费失败但发回Broker成功），需要将其从消息处理队列中移除，同时返回ProcessQueue中最小的offset，使用这个offset值更新消费进度
        // 1 一是已经没有消息了，返回 ProcessQueue最大offset+1
        // 2 二是还有消息，则返回未消费消息的最小offset
        // 例子：ProcessQueue中有offset为101-110的10条消息，如果全部消费完了，返回的offset为111；如果101未消费完成，102-110消费完成，则返回的offset为101，这种情况下如果消费者异常退出，会出现重复消费的风险，所以要求消费逻辑幂等。
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());

        // 更新当前消费端的 OffsetStore 中维护的 offsetTable 中的消费位移，offsetTable 记录每个 messageQueue 的消费进度。
        // 这里只是更新内存数据，而将offset上传到broker是由定时任务执行的。MQClientInstance.start()会启动客户端相关的定时任务。
        // updateOffset()的最后一个参数increaseOnly为true，表示单调增加，新值要大于旧值
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            // 更新 Consumer 的偏移量
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 消费不成功，进行重发消息
     *
     * @param msg
     * @param context
     * @return
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {

        // 1、注意这里：默认为0，其实一直都是0，其它地方没有修改。这表示RocketMQ延迟消息的 延迟级别
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            // 2、发送给Broker
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    /**
     * 提交延迟消费请求，5s
     *
     * @param msgs         消息列表
     * @param processQueue 消息处理队列
     * @param messageQueue 消息队列
     */
    private void submitConsumeRequestLater(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交延迟消费请求
     *
     * @param consumeRequest
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 消费请求任务体
     */
    class ConsumeRequest implements Runnable {
        /**
         * 消费消息集合
         */
        private final List<MessageExt> msgs;
        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            // 废弃处理队列不进行消费
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            // 1 获取监听器，即 Consumer 中设计的回调方法
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;

            // 消费 Context
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            // 消费结果状态
            ConsumeConcurrentlyStatus status = null;

            // todo 当消息为重试消息，设置 Topic 为原始 Topic
            // todo 疑问：重试消息主题都变了，消费者怎么拉取？因为消费者在订阅 Topic 时，还会自动订阅对应的重试主题，因此可以拉取到
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            // Hook
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            // 开始消费时间
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                // 设置开始消费时间
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                // 回调 Consumer 中的监听回调方法，进行消费，消费拉取到的消息
                // todo 执行业务代码中监听器的消息逻辑
                // todo status 为使用方返回的消费结果
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);

                // 业务方消费的时候可能抛出异常
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                        RemotingHelper.exceptionSimpleDesc(e),
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                // 消费异常
                hasException = true;
            }


            // 消费时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            // 解析消费返回结果类型
            if (null == status) {

                // 有异常
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;

                    // 返回为 null
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }

                // 如果消费时长 >= 15min
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;

                // 如果是稍后重新消费
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;

                // 消费成功
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            // 消费结果状态为空时(可能出现异常了)则设置为稍后重新消费
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }


            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            // 统计
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                    .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 处理消费结果

            // 如果消费处理队列被置为无效，恰好消息被消费，则可能导致消息重复消费
            // 因此，消息消费要尽最大可能性实现幂等性

            // todo 处理消费结果
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
