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
package org.apache.rocketmq.client.consumer;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 1 RocketMQ 拉模式，消费者不自动向消息服务器拉取消息，而是将控制权交给了应用程序，RocketMQ 拉模式下的消费者只是提供拉取消息 API。
 * 2 PUSH 模式相关点：
 * - 消息拉取机制：PullMessageServer 线程根据PullRequest拉取任务循环拉取；
 * - 消息队列负载机制，按照消费组，对主题下的消息队列，结合当前消费组内消费者数量动态负载。
 *
 *
 * <p>
 * Default pulling consumer.
 * This class will be removed in 2022, and a better implementation {@link DefaultLitePullConsumer} is recommend to use
 * in the scenario of actively pulling messages.
 */
@Deprecated
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {

    protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    /**
     * Do the same thing for the same Group, the application must be set,and guarantee Globally unique
     * <p>
     * 消费组名称
     */
    private String consumerGroup;
    /**
     * Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
     * 长轮询模式下挂起的最大超时时间，在Broker端根据偏移量从存储文件中查找消息时如果返回 PULL_NOT_FOUND时，不立即返回给拉取客户端，而是交给PullRequestHoldService线程，
     * 每隔5秒再去拉取一次消息，如果找到则返回给消息拉取客户端，否则超时。
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;
    /**
     * Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not
     * recommended to modify
     * 整个消息拉取过程中，拉取客户端等待服务器响应结果的超时时间，默认30s
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
    /**
     * The socket timeout in milliseconds
     * 默认10s,拉消息时建立网络连接的超时时间
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    /**
     * Consumption pattern,default is clustering
     * 消费模式，默认集群消费
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Message queue listener
     */
    private MessageQueueListener messageQueueListener;
    /**
     * Offset Storage
     * <p>
     * 消息消费进度管理器
     */
    private OffsetStore offsetStore;
    /**
     * Topic set you want to register
     * 订阅的主题列表
     */
    private Set<String> registerTopics = new HashSet<String>();
    /**
     * Queue allocation algorithm
     * 队列分配器
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;
    /**
     * 消息重试最大次数，默认 16 次
     */
    private int maxReconsumeTimes = 16;

    public DefaultMQPullConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    public DefaultMQPullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    public DefaultMQPullConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    public DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(null, consumerGroup, rpcHook);
    }

    public DefaultMQPullConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, null);
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook.
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook       RPC hook to execute before each remoting command.
     */
    public DefaultMQPullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, rpcHook);
    }

    /**
     * 消费者启动
     *
     * @throws MQClientException
     */
    @Override
    public void start() throws MQClientException {
        this.setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPullConsumerImpl.start();
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public Set<String> getRegisterTopics() {
        return registerTopics;
    }

    public void setRegisterTopics(Set<String> registerTopics) {
        this.registerTopics = withNamespace(registerTopics);
    }

    /**
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }


    @Override
    public void shutdown() {
        this.defaultMQPullConsumerImpl.shutdown();
    }

    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
        synchronized (this.registerTopics) {
            this.registerTopics.add(withNamespace(topic));
            if (listener != null) {
                this.messageQueueListener = listener;
            }
        }
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, timeout);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback,
                     long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
                     PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
                     PullCallback pullCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
                                    PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
    }

    @Override
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
    }

    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
    }

    @Override
    public MessageExt viewMessage(String topic,
                                  String uniqKey) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(uniqKey);
            return this.viewMessage(uniqKey);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), uniqKey);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public DefaultMQPullConsumerImpl getDefaultMQPullConsumerImpl() {
        return defaultMQPullConsumerImpl;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }
}
