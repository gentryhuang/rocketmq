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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 1 pull与Push在RocketMQ中，其实就只有Pull模式，所以Push其实就是用pull封装一下，在消费端开启一个线程 PullMessageService 循环向 Broker 拉取消息，
 * 一次拉取任务结束后，默认立即发起另一次拉取操作，实现准实时自动拉取。
 * 2 push 模式是基于发布订阅模式的，pull 模式是基于消息队列模式的。
 */
public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;

    /**
     * 消息队列 与 拉取 Broker ID 的映射
     * todo 存放的是建议 消息队列从从哪个 Broker 服务器拉取消息的缓存表
     * 1 当拉取消息时，会通过该映射获取拉取请求对应的 Broker
     * 2 当处理拉取结果时，会更新该表，根据 Broker 建议下次从那个 broker 拉取消息
     */
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    /**
     * 是否使用默认 Broker
     */
    private volatile boolean connectBrokerByUser = false;
    /**
     * 默认 Broker 编号 ，是主 Broker
     */
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 处理拉取结果
     * 1. 更新 消息队列拉取消息 Broker 编号的映射
     * 2. 解析消息，并根据订阅信息消息 tagCode 匹配合适消息，即会过滤消息
     * <p>
     * todo 特别说明：
     * 1） 基于 Tag 模式会在服务端拉取消息的过程先过滤一次（过滤过程不会访问 CommitLog 数据，可以保证高效过滤），使用的是 tag 的 hashCode ，但是不是绝对的准确，
     * 在消费端又根据 tag 进行过滤。（为什么在 Broker 服务端不使用 tag 过滤呢？ Message Tag 其实是字符串形式，ConsumeQueue 中存储的是其对应的 hashCode，
     * 是为了在 ConsumeQueue 中定长存储，节省空间）；
     * 2） 采用 Broker 端和消费端都过滤，这样的情况下，即使存在 Tag 的 hash 冲突，也可以在 Consumer 端进行修正，保证正确性。
     *
     * @param mq               消息队列
     * @param pullResult       拉取结果
     * @param subscriptionData 订阅信息
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq,
                                        final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        /**
         * todo 根据拉取结果中建议，更新消息队列拉取消息 Broker 编号的映射
         * 即 在处理拉取结果时会将服务端建议的brokerId更新到broker拉取缓存表中
         */
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        // 解析消息，并根据订阅信息消息 tagCode 匹配合适消息
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            // 解析消息
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            // todo 包含 offsetMsgId 的生成
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            // todo 根据订阅信息消息tag 匹配合适消息
            List<MessageExt> msgListFilterAgain = msgList;

            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());

                // todo 遍历拉取的消息，根据订阅的 tag 过滤，留下消费方关注的消息
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        // tag 过滤
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            // Hook
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            // 设置消息队列当前最小/最大位置到消息拓展字段
            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }

            // 设置消息列表
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 清空消息二进制数组
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * 更新消息队列到Broker 的映射
     *
     * @param mq
     * @param brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * 拉取消息核心方法
     *
     * @param mq                         消息队列
     * @param subExpression              订阅表达式
     * @param expressionType             过滤类型
     * @param subVersion                 订阅版本号
     * @param offset                     拉取队列开始位置
     * @param maxNums                    拉取消息数量
     * @param sysFlag                    拉取请求系统标识
     * @param commitOffset               提交到 Broker 的消费进度
     * @param brokerSuspendMaxTimeMillis Broker 挂起请求最大时间，默认是 15s
     * @param timeoutMillis              请求 Broker 超时时长
     * @param communicationMode          通讯模式
     * @param pullCallback               拉取回调
     * @return 拉取消息结果，只有通讯模式为同步时，才返回结果，否则返回null
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        // todo 获取 Broker 信息，从 消息队列 与 拉取 Broker ID 的映射表中选择具体哪个 BrokerId (MessageQueue 中只包含 BrokerName)
        FindBrokerResult findBrokerResult =

                // 根据 brokerName 和 brokerId 获取 Broker 信息
                // todo 在整个 RocketMQ Broker 的部署结构中，相同名称的 Broker 构成主从结构，其 BrokerId 会不一样,主服务器的brokerId为0，从服务器的brokerId大于0。
                //  在每次拉取消息后，会给出一个建议，下次是从主节点还是从节点拉取
                this.mQClientFactory.findBrokerAddressInSubscribe(
                        mq.getBrokerName(),
                        // 获取 mq 拉取消息对应的 Broker 编号
                        this.recalculatePullFromWhichNode(mq),
                        false);

        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false);
        }

        // 请求拉取消息
        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }

            // 如果推荐的是从节点，那么就不能再提交消费进度了
            int sysFlagInner = sysFlag;
            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            // 创建拉取消息请求 Header 对象
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            // 拉取消息的位置
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);

            // 拉取消息时的系统标志
            requestHeader.setSysFlag(sysFlagInner);

            // todo 提交到 Broker 的消费进度
            requestHeader.setCommitOffset(commitOffset);
            // todo Broker 取消息时暂停时间（没有消息会等待的时间），默认 15s
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);

            // todo 设置订阅表达式
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            // 获取 Broker 地址
            String brokerAddr = findBrokerResult.getBrokerAddr();

            // 如果消息过滤模式为 类过滤，则需要根据主题名称、Broker 地址找到注册在 Broker 上的 FilterServer 地址，
            // 从 FilterServer 上拉取消息，否则从 Broker 上拉取消息
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                // 计算从哪个 FilterServer 中拉取消息
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            // 网络通信，拉取消息，根据不同的通信模式，可以是异步也可以是同步
            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                    brokerAddr,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback);

            return pullResult;
        }

        // 当 Broker 信息不存在，则抛出异常
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * 获取消息队列拉取消息对应的 Broker 编号
     *
     * @param mq
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        // 若开启默认 Broker 开关，则返回默认 Broker 编号，即主节点
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        // 若消息队列映射拉取Broker存在，则返回映射Broker编号
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        // 未找到，则返回 brokerName 的主节点
        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
