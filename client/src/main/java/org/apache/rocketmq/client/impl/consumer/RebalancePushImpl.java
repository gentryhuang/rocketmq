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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                             MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    /**
     * 移除不需要的消息队列相关的信息，并返回是否移除成功。
     * <p>
     * 1. 持久化消费进度，并移除
     * 2. 顺序消费 & 集群模式，解锁对该队列的锁定
     *
     * @param mq 消息队列
     * @param pq 消息处理队列
     * @return 是否移除成功
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {

        // 同步队列的消费进度，并移除指定队列的缓存消费进度
        // todo 为啥没有操作消息队列或消息处理队列？？？ 这里对消费进度进行处理
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);

        // 集群模式下，顺序消费移除时，解锁对队列的锁定
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {

                // 获取消息队列消费锁（JVM 锁），避免和消息队列消费冲突。
                // 如果未获得锁而进行操作，可能会导致消息无法严格顺序消费
                if (pq.getConsumeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {

                        // 获得 JVM 锁后，延迟解锁 mq
                        return this.unlockDelay(mq, pq);
                    } finally {
                        pq.getConsumeLock().unlock();
                    }
                } else {
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                            mq,
                            pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    /**
     * 延迟解锁 Broker 消息队列锁
     *
     * @param mq
     * @param pq
     * @return
     */
    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        // 如果消息处理队列不存在消息，则直接解锁。否则延迟解锁
        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);

                    // 解锁 mq
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Deprecated
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1L;
        try {
            result = computePullFromWhereWithException(mq);
        } catch (MQClientException e) {
            log.warn("Compute consume offset exception, mq={}", mq);
        }
        return result;
    }

    /**
     * 计算消息队列开始消费位置
     *
     * @param mq 消息队列
     * @return
     * @throws MQClientException
     */
    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        long result = -1;
        // 从何处开始消费
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();

        // 消费进度存储
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();

        /**
         * 对于 lastOffset、maxOffset、时间戳查找 offset 都是通过 MQClientAPIImpl 提供的接口进行查询的，
         * MQClientAPIImplclient对broker请求的封装类，使用Netty进行异步请求，对应的RequestCode分别为：
         * - RequestCode.QUERY_CONSUMER_OFFSET
         * - RequestCode.GET_MAX_OFFSET
         * - RequestCode.SEARCH_OFFSET_BY_TIMESTAMP。
         */
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:

                // 一个新的消费集群第一次启动从队列的最后位置开始消费。后续再启动接着上次消费的进度开始消费。
            case CONSUME_FROM_LAST_OFFSET: {

                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            // 获取消费队列最大 offset
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }

            // 个新的消费集群第一次启动从队列的最前位置开始消费。后续再启动接着上次消费的进度开始消费。
            case CONSUME_FROM_FIRST_OFFSET: {
                // 先查询当前消费队列消费进度
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;

                    // 当前消费队列消费进度小于0，则从0开始
                } else if (-1 == lastOffset) {
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }

            // 一个新的消费集群第一次启动从指定时间点开始消费。后续再启动接着上次消费的进度开始消费。
            case CONSUME_FROM_TIMESTAMP: {
                // 查询当前消费队列消费进度
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    } else {
                        try {

                            // 获取consumer启动时间
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                    UtilAll.YYYYMMDDHHMMSS).getTime();

                            // 根据时间戳获取offset信息
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }

        return result;
    }

    /**
     * 发起消息拉取请求。
     * <p>
     * 说明：
     * 将消息拉取请求 PullRequest 加入到消息拉取队列中，有后台线程不断从队列中取出拉取请求，然后根据请求信息从 Broker 拉去消息，进而交给消费者消费
     *
     * @param pullRequestList
     */
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        // 加入到拉取集合中，等待执行
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }
}
