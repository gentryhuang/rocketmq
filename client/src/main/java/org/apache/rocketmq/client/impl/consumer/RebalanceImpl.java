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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 均衡消息队列服务，负责分配当前消费组下的 Consumer 可消费的消息队列( MessageQueue )
 * todo 特别说明：
 * 1 该实例中的 Topic 的队列信息是动态的，随着 Consumer 的变化而变化，数据是全量的，因为要为每个 Consumer 分配队列
 * 2 每个 Consumer 都持有该实例，用于给当前 Consumer 分配队列。
 */
public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    /**
     * 消息队列 到 消息处理队列的映射
     */
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);

    /**
     * 订阅 Topic 下的消息队列
     * 1) 从 NameSrv 更新路由配置到本地，设置该属性的情况如下（todo 注意：起始的时候是全部的队列，主要作为分配队列的数据源）：
     * 2) 包括重试主题对应的队列信息，默认情况下，重试主题只有一个队列。具体的重试主题队列信息分布在哪个 Broker，要看重试消息发送到哪个 Broker 上了，和消息的消费队列有关。
     *
     * @see MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)
     */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<String, Set<MessageQueue>>();

    /**
     * 订阅数据信息
     * todo 注意：每个 Topic 都对应一个重试的 Topic。也就是，消费者在订阅时，会自动自订阅 Topic 对应的重试主题
     *
     * @see DefaultMQPushConsumerImpl#copySubscription()
     */
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<String, SubscriptionData>();

    /**
     * 消费者组
     * todo 根据消费组做负载：1）获取消费组下的消费者们 2）对于顺序消费，在 Broker 上的分布式的一个隔离标志
     */
    protected String consumerGroup;

    /**
     * 消息模式
     */
    protected MessageModel messageModel;

    /**
     * 分配消息队列的策略，默认平均分配策略
     */
    /* 负载算法的具体实现，究竟如何分配就是由这个总指挥决定的 */
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * 消息客户端实例
     */
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
                         AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                         MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 向 Broker 解锁指定的消息队列 mq
     *
     * @param mq
     * @param oneway
     */
    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            // 以消费组进行隔离
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                // 向 Broker 发起解锁 mq 请求
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                        this.consumerGroup,
                        this.mQClientFactory.getClientId(),
                        mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    /**
     * 解锁当前消费客户端实例的所有消费队列
     *
     * @param oneway
     */
    public void unlockAll(final boolean oneway) {
        // 获取当前消费客户端实例分配的所有队列以及这些队列所在的 Broker
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            // 队列所在的 Broker
            final String brokerName = entry.getKey();
            // 队列
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            // 根据 BrokerName 获取 Broker
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                // 构建解锁队列的请求体
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                // 当前消费者所在的消费组
                requestBody.setConsumerGroup(this.consumerGroup);
                // 当前消费客户端实例ID
                requestBody.setClientId(this.mQClientFactory.getClientId());
                // 要解锁的队列集合
                requestBody.setMqSet(mqs);

                try {
                    // 向 Broker 请求解锁 mqs
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    // 将消费端分配到的消息队列对应的 ProcessQueue 标记解除锁定
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            // 解除锁定
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 从消息处理队列中分离出，以 Broker 为 key 的 消息队列集合
     *
     * @return
     */
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    /**
     * 锁定消息队列 mq
     * 说明：
     * 1 请求 Broker 获得指定消息队列的分布式锁，即锁定指定的消息队列
     * 2 Broker 消息队列锁定会过期，默认配置 30s,因此，Consumer 需要不断向 Broker 刷新该锁过期时间，默认配置 20s 刷新一次。
     *
     * @param mq 消息队列
     * @return 是否成功
     */
    public boolean lock(final MessageQueue mq) {
        // 获取 mq 所在的 Broker 地址（主节点）
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {

            // 锁定 MessageQueue 的请求体
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            // 以消费组进行隔离
            requestBody.setConsumerGroup(this.consumerGroup);
            // 消费组下的消费者客户端实例
            requestBody.setClientId(this.mQClientFactory.getClientId());
            // 要锁定的 MessageQueue 集合
            requestBody.getMqSet().add(mq);

            try {

                // 请求 Broker 获得指定消息队列的分布式锁
                Set<MessageQueue> lockedMq =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                // 遍历 Broker 锁定的消息队列
                for (MessageQueue mmqq : lockedMq) {
                    // 获取消息队列关联的消息处理队列
                    // 设置消息处理队列锁定成功。
                    // 锁定消息队列成功，可能本地没有消息处理队列，设置锁定成功会在 lockAll()方法。
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                // 判断执行的消息队列是否已被锁定
                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                        lockOK ? "OK" : "Failed",
                        this.consumerGroup,
                        mq);

                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    /**
     * Consumer 不断向 Broker 刷新锁消息队列锁过期时间。
     * todo 特别说明：
     * 每个 MQ 客户端，会定时发送 LOCK_BATCH_MQ 请求，并且在本地维护获取到锁的所有队列，即在消息处理队列 ProcessQueue 中使用 locked 和 lastLockTimestamp 进行标记。
     */
    public void lockAll() {

        // 根据当前 Consumer 缓存的消息处理队列解析出消费队列
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        // 遍历
        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            // 根据 BrokerName 获取 Broker 主节点的地址信息
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);

            // 向对应的 Broker 发送锁定消息队列的请求
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                // 那个消费组
                requestBody.setConsumerGroup(this.consumerGroup);
                // 当前消费者标识
                requestBody.setClientId(this.mQClientFactory.getClientId());
                // 锁住哪些消费队列
                requestBody.setMqSet(mqs);

                try {
                    // todo 向 Broker 请求锁定 mqs 队列，该方法会返回本次成功锁定的消息消费队列
                    Set<MessageQueue> lockOKMQSet =
                            this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    // 遍历 Broker 锁定后的结果，用于更新当前消费者缓存的 ProcessQueue 的 locked 状态
                    for (MessageQueue mq : lockOKMQSet) {
                        // 消息处理队列映射中包含该锁定的消息队列，则锁定消息队列
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            // todo 标记锁定
                            processQueue.setLocked(true);
                            // 设置锁定时间
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }

                    // 没有锁定的情况
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                // 标记非锁定
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 为 Consumer 分配队列
     * 说明：当前 RebalanceImpl 是某个 Consumer 持有的
     *
     * @param isOrder 是否顺序消息
     */
    public void doRebalance(final boolean isOrder) {
        // todo 获取 Consumer 订阅数据，以 Topic 维度。注意每个 Topic 自动对应一个重试主题
        // 分配 Topic 下的队列
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        if (subTable != null) {
            // todo 遍历消费者订阅数据对每个主题的队列进行负载。 以订阅数据为基准，进行队列的分配
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                // 获取 Topic
                final String topic = entry.getKey();
                try {

                    // 根据 Topic 和 是否有序，进行分配队列。即 分配每一个 Topic 的消息队列。
                    this.rebalanceByTopic(topic, isOrder);

                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        // 移除未订阅 Topic 对应的消息队列，即清除 processQueueTable 缓存中的无效队列映射
        // todo 重要，队列重分配后要把无效队列清除掉
        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 分配队列
     * <p>
     * 根据不同的消息模型，分配队列的流程也不同：
     * 1 广播模式下，分配 Topic 的所有读队列
     * 2 集群模式下，分配 Topic 的部分读队列
     *
     * @param topic   消费组订阅的 Topic
     * @param isOrder 是否有序
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        // 根据不同的消费模型，进行不同的处理
        switch (messageModel) {

            // todo 广播模式
            case BROADCASTING: {

                // 根据 Topic 获取对应的消息队列，即缓存中订阅该 Topic 分配的队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);

                if (mqSet != null) {
                    // todo 传入的是所有队列
                    // 更新该 Topic 下的队列，并返回是否改变
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);

                    // 发生改变，调整主题下各个队列的拉取阈值，以及向 Broker 发送心跳
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                                consumerGroup,
                                topic,
                                mqSet,
                                mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }

                break;
            }

            // 集群模式
            case CLUSTERING: {

                // 获取该Topic主题下的消息消费队列集合（mqSet），即读队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);

                // todo 向Broker端发送获取订阅 topic 的消费组 consumerGroup 下的所有消费者ID列表的 RPC 通信请求
                // （Broker端基于前面Consumer端上报的心跳包数据而构建的consumerTable做出响应返回，业务请求码：GET_CONSUMER_LIST_BY_GROUP）；
                // todo 这里是订阅关系不一致发生消息丢失的一个问题点，在订阅时要保证同一个消费组下消费者们的订阅关系一致
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);

                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    // 排序 消息队列 和 消费者数组。
                    // todo 因为是在Client进行分配队列，排序后，各Client的顺序才能保持一致。
                    // 这样一来尽管各个Consumer在负载均衡的时候不进行任何信息交换，但是却可以互不干扰有条不紊的将队列均衡的分配完毕。
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    // 消费队列分配策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    // 分配后的结果
                    List<MessageQueue> allocateResult = null;
                    try {
                        // todo 根据 队列分配策略（默认为：消息队列的平均分配算法）分配消息队列给消费者组 consuemrGroup 下的消费者分配队列
                        // 即 计算出待拉取的消息队列分配到当前 Consumer
                        allocateResult = strategy.allocate(
                                this.consumerGroup,
                                this.mQClientFactory.getClientId(),
                                mqAll,
                                cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                                e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    // todo 传入的是通过队列分配算法为当前消费者分配的队列集合
                    // todo 更新消息队列
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);

                    // 发生改变，调整主题下各个队列的拉取阈值，以及向 Broker 发送心跳
                    if (changed) {
                        log.info(
                                "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                                allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    /**
     * 移除未订阅的消息队列
     */
    private void truncateMessageQueueNotMyTopic() {
        // 获取订阅信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        // 遍历
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            // 当前消息队列未被订阅，则从缓存中移除 todo 消费客户端持有的队列必须对应订阅的 Topic，否则是无效队列
            // 注意：DefaultMQPushConsumer#unsubscribe(topic) 时，只移除订阅主题集合( subscriptionInner )，对应消息队列移除在该方法。
            if (!subTable.containsKey(mq.getTopic())) {
                // 移除无效队列
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    // 废弃消息处理队列
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * todo 当负载均衡时，更新当前消费客户端的消息队列。判断该消费者的队列是否发生变化
     * 遍历 processQueueTable 当前负载队列缓存集合
     * - 移除在processQueueTable && 不存在于 mqSet 里的 消息队列 - 以新的为准
     * - 增加不在processQueueTable && 存在于mqSet 里的 消息队列 - 以新的为准
     * - 即 以 mqSet 为准，
     * <p>
     * 特别说明：
     * 1 该方法本质是更新当前消费端 分配到的消息队列 与消息处理队列的映射
     * 2 对于当前消费端来说，如果是一个新的消费队列，那么会创建一个拉取消息的请求 PullRequest 对象，用于后续从 Broker 中不断拉取消息队列中消息；以最新分配的队列为准，删除最新队列外的缓存队列。
     * 3 todo 是不是消费端的 消息处理队列缓存大小（当前分配到的消息队列） <= 消息队列大小（消息队列缓存是针对 Topic 初始化的所有消息队列集合）
     * <p>
     * todo 关键一点：
     * 消息消费队列在同一消费组不同消费者之间的负载均衡，其核心设计理念是一个消息消费队列在同一时间只允许被同一消费组内的一个消费者消费，一个消息消费者能同时消费多个消息队列。
     *
     * @param topic   订阅的 topic
     * @param mqSet   负载均衡结果后的消息队列数组（属于订阅 topic 下的队列）
     * @param isOrder 是否有序
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {
        boolean changed = false;

        // 1 移除在 processQueueTable 但 不存在于 mqSet 中的消息队列 todo 毕竟要以新的为准
        // todo 这种情况说明，不在 mqSet 集合中的 mq ，经过本次消息队列负载后，被分配给其他消费者了，需要暂停该消息队列消息的消费。
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            // 消息队列
            MessageQueue mq = next.getKey();
            // 消息处理队列
            ProcessQueue pq = next.getValue();

            // 判断消费者订阅的 Topic 和缓存中分配的队列是否匹配，如果匹配
            // 即只处理 mq 的主题与传入主题相关的 ProcessQueue
            if (mq.getTopic().equals(topic)) {

                // 该 Topic 分配的消息队列不包含 mq，说明该 mq 对应的消息处理队列不可用，废弃它
                if (!mqSet.contains(mq)) {

                    // todo 废弃 ProcessQueue
                    pq.setDropped(true);

                    // todo 将 mq 消费进度持久化后，移除缓存中 mq 的消费进度信息。如果是顺序消费，还需尝试释放 mq 的分布式锁，释放依据是：没有正在消费该队列
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {

                        // todo 移除缓存中的该消息队列到消息处理队列的映射关系
                        it.remove();

                        // 标记当前 topic 下队列分配需要发生变化
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }

                    // 分配到的消息队列集合包含缓存中的消息队列，还需要判断缓存中的消息处理队列是否过期，即
                    // 队列拉取消息超时，即 当前时间 - 最后一次拉取消息时间 > 120s ( 120s 可配置)，判定发生 BUG，过久未进行消息拉取，移除消息队列
                    // todo 最后一次拉取消息时间的更新，是在 pullMessage 方法中更新的，主要用来判断 PullMessageService 是否空闲
                } else if (pq.isPullExpired()) {
                    switch (this.consumeType()) {
                        // 在 Pull 模式下不用管，因为 Pull 模式的拉取时间间隔是由业务方控制的
                        case CONSUME_ACTIVELY:
                            break;

                        // 在 Push 模式下，标记消息处理队列不可用，并尝试移除
                        case CONSUME_PASSIVELY:

                            // todo 废弃 ProcessQueue，及时阻止继续向 ProcessQueue 中拉取消息
                            pq.setDropped(true);

                            // todo 将 mq 消费进度持久化后，移除缓存中 mq 的消费进度信息。
                            // todo 如果是顺序消费，则尝试释放 mq 的分布式锁，释放依据是：没有正在消费该队列
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                        consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // 2 增加不在 processQueueTable 且 存在 mqSet 中的消息队列（注意，此时 processQueueTable 中的元素是过滤后的结果，里面的队列都是有效的）
        // todo 为新的 MessageQueue 创建对应的 PullRequest ，用于从对应的消息队列中拉取消息
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();


        /**
         * 为过滤后的消息队列集合（mqSet）中的每个MessageQueue创建一个ProcessQueue对象并存入RebalanceImpl的processQueueTable队列中。
         * 1 调用RebalanceImpl实例的computePullFromWhere(MessageQueue mq)方法获取该MessageQueue对象的下一个进度消费值offset (逻辑偏移量)
         * 2 将1 中得到的 offset 填充至创建的pullRequest对象属性中
         * 3 将 2 中创建的 pullRequest 添加到拉取列表—pullRequestList中
         * 4 执行dispatchPullRequest()方法，将Pull消息的请求对象PullRequest依次放入PullMessageService服务线程的阻塞队列pullRequestQueue中，待该服务线程取出后向Broker端发起Pull消息的请求。
         * 5 其中，可以重点对比下，RebalancePushImpl和RebalancePullImpl两个实现类的dispatchPullRequest()方法不同，RebalancePullImpl类里面的该方法为空
         */
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {

                /**
                 * todo 说明：
                 * 1 顺序消费时，锁定消息队列。如果锁定失败，就不会新增消息处理队列，因为此时虽然消费队列 mq 分配给了当前消费者，但是该消费组下存在其它的消费者还在使用该队列，如果当前消费者也开始通过该消费队列拉取消息，那么就可能导致乱序消费。
                 * 也就是在顺序消费时，只有锁定队列才能拉取消息。
                 * 2 问题：发生消息队列重新负载时，原先由自己处理的消息队列被另外一个消费者分配，此时如果还未来的及将ProceeQueue解除锁定，就被另外一个消费者添加进去，此时会存储多个消息消费者同时消费个消息队列？
                 *   答案：不会的，因为当一个新的消费队列分配给消费者时，在添加其拉取任务之前必须先向Broker发送对该消息队列加锁请求，只有加锁成功后，才能添加拉取消息，否则等到下一次负载后，该消费队列被原先占有的解锁后，
                 *   才能开始新的拉取任务。但是并发消费可能会出现这种情况的。
                 */
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                // todo 移除消费队列进度缓存，即删除 messageQueue 旧的 offset 信息
                // todo 防御性编程？
                this.removeDirtyOffset(mq);

                // 创建消息处理队列
                ProcessQueue pq = new ProcessQueue();

                long nextOffset = -1L;
                try {

                    // todo 从 Broker 获取队列消费进度，并更新到内存中
                    nextOffset = this.computePullFromWhereWithException(mq);
                } catch (MQClientException e) {
                    log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                    continue;
                }


                // todo 只有消费进度 >= 0 才认为消费进度正常，等于 -1 就不正常，此时就不能拉取消息
                if (nextOffset >= 0) {
                    // 添加新消费处理队列
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);

                        // todo 如果是订阅的新消费队列，则为该消息队列创建一个 PullRequest 拉取消息的对象，用于从 Broker 拉取消息，然后交给当前消费者消费
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);

                        //创建 PullRequest 对象
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);

                        // todo 待拉取的 MessageQueue 逻辑偏移量
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);

                        // 添加消费拉取消息请求
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        // 派发拉取消息请求
        // todo 注意：这是拉取消息的起点，即每个消费队列对应一个 PullRequest
        // todo 消息拉取由PullMessageService线程根据这里的拉取任务进行拉取
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                                             final Set<MessageQueue> mqDivided);

    /**
     * 移除无用的消息队列
     *
     * @param mq
     * @param pq
     * @return
     */
    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     *
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    /**
     * 发起消息拉取请求。该调用是PushConsumer不断不断不断拉取消息的起点
     *
     * @param pullRequestList
     */
    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
