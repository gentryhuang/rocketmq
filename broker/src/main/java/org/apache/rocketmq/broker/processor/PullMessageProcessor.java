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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 拉取消息处理
 */
public class PullMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理拉取消息请求
     *
     * @param channel            网络通道
     * @param request            消息拉取请求
     * @param brokerAllowSuspend 是否允许挂起，也就是是否允许在未找到消息时暂时挂起线程。第一次调用时默认为true。
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand processRequest(
            final Channel channel, // 网络通道
            RemotingCommand request,
            boolean brokerAllowSuspend)
            throws RemotingCommandException {

        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();

        // 解码拉取消息请求头
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());

        log.debug("receive PullMessage request command, {}", request);

        // 校验 broker 是否可读，不可读，就不能拉取消息
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pulling message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        /*--- 消费者向 Broker 发起消息拉取请求时，如果broker上并没有存在该消费组的订阅消息时，
              如果不允许自动创建(autoCreateSubscriptionGroup 设置为 false)，默认为true，则不会返回消息给客户端 -----*/

        // todo 校验 consumer 分组配置是否存在。当不存在时，如果允许自动创建则根据当前 consumerGroup 创建一个基本的消费组配置信息.
        // 目前好像只有控制台可以修改，程序中都是自动创建。
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());

        // todo 如果还是为空，则不能拉取消息，直接报错 订阅组不存在
        // todo 如果不允许自动创建订阅组信息，必须手动向 Broker 创建订阅组信息，否则不能拉取消息
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        // 校验 consumer 分组是否可消费，默认都是可以消费的，除非通过控制台修改
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        // todo 当没有消息时是否挂起请求
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        // todo 是否提交消费进度（即消费端上报本地的消费进度过来了，要不要接受）
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        // todo 是否过滤订阅表达式(subscription)
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        // todo 如果没有消息时挂起请求，则获取挂起请求超时时长
        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        // todo 校验 topic 配置存在
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        // 校验 topic配置 权限可读
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        // todo 校验 读取队列 在 topic配置 队列范围内
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                    requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        /*----------------------- 构建过滤器 -----------------------*/
        /*
           RocketMQ 消息过滤有两种模式：
           1. 类过滤 ClassFilterModel 和 表达式模式(Expression)，其中表达式模式又可分为 ExpressionType.TAG 和 ExpressionType.SQL92
           2. TAG过滤，在服务端拉取时，会根据 ConsumeQueue 条目中存储的 tag hashcode 与订阅的 tag (hashcode 集合)进行匹配，匹配成功则放入待返回消息结果中，然后在消息消费端（消费者，还会对消息的订阅消息字符串进行再一次过滤。
               为什么需要进行两次过滤呢？
               2.1 为什么不在服务端直接对消息订阅 tag 进行匹配呢？
                  主要就还是为了提高服务端消费消费队列（文件存储）的性能，如果直接进行字符串匹配，那么 consumequeue 条目就无法设置为定长结构，检索 consuequeue 就不方便。
               2.2 为什么不只在消费端进行过滤呢？
                  在服务端根据 Tag hash 可以快速过滤掉不符合过滤条件的消息索引，尽可能避免从 CommitLog 获取消息。
         */

        /*----------------------- 构建过滤器 -----------------------*/


        // Tag 过滤信息
        SubscriptionData subscriptionData = null;
        // ConsumerFilterData 过滤消息对象
        ConsumerFilterData consumerFilterData = null;

        // todo 是否要过滤订阅表达式，如 consumer.subscribe("TopicA","tag1");
        if (hasSubscriptionFlag) {
            try {
                // todo 根据拉取消息请求中的 topic、subscription、expressionType ，构建 SubscriptionData 订阅信息，以过滤消息
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType());

                // 判断是否是 Tag 过滤，主要针对 SQL92 模式
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {

                    // SQL92 模式，则 构建 ConsumerFilterData  过滤对象
                    consumerFilterData = ConsumerFilterManager.build(
                            requestHeader.getTopic(),
                            requestHeader.getConsumerGroup(),
                            requestHeader.getSubscription(),
                            requestHeader.getExpressionType(),
                            requestHeader.getSubVersion()
                    );

                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                log.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(),
                        requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }


            // 无子订阅模式，走的是 ClassFilter 过滤模式
        } else {
            // 校验 消费分组信息 是否存在
            // todo 客户端，包括消息生产者和消息消费者都会定时向 Broker 心跳
            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            // 校验 消费分组信息 消息模型是否匹配
            if (!subscriptionGroupConfig.isConsumeBroadcastEnable()
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }

            // todo  校验 订阅信息 是否存在。注意，同一个消费组下，同一个 Topic 的订阅信息只有一份，以最后上报的消费者为准（版本更新）
            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            // 校验 订阅信息版本 是否合法，不能比请求的订阅信息版本低
            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                        subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }

            // 走 ClassFilter 模式
            // 直接从brokerController.getConsumerFilterManager() 中根据 topic、consumerGroup或取，如果取不到直接提示错误。
            // 因为消费方使用的是 subscribe(String topic, String fullClassName, String filterClassSource) 方法时进行订阅的，此时会立即上报到 Broker 中，并随着定时任务和心跳一样不断上报
            if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                if (consumerFilterData == null) {
                    response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
                    response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return response;
                }
                if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
                    log.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
                    response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
                    response.setRemark("the consumer's consumer filter data not latest");
                    return response;
                }
            }
        }

        // 如果非 Tag 过滤，需要打开 enablePropertyFilter = true
        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())
                && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        // todo 构建消息过滤器，用于在拉取消息时进行过滤不符合订阅的消息
        MessageFilter messageFilter;

        // 支持对重试主题的过滤
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData,
                    this.brokerController.getConsumerFilterManager());

            // 不支持对重试主题的过滤，创建表达式模式过滤
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData,
                    this.brokerController.getConsumerFilterManager());
        }

        // todo 调用 MessageStore 获取消息的方法，查找消息
        // todo 根据多种情况，返回不同的状态
        final GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(
                requestHeader.getConsumerGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId(),
                requestHeader.getQueueOffset(),
                requestHeader.getMaxMsgNums(),
                messageFilter // 消息过滤器
        );


        // todo 根据获取消息的结果填充 response
        if (getMessageResult != null) {
            // 设置拉取消息请求的响应状态
            response.setRemark(getMessageResult.getStatus().name());
            // 下次从哪个偏移量开始拉取消息
            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            // 当前 ConsumeQueue 的最小逻辑偏移量
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            // 当前 ConsumeQueue 的最大逻辑偏移量
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());

            // todo 根据获取消息结果中是否建议从 slave 拉取消息，来设置 BrokerId
            if (getMessageResult.isSuggestPullingFromSlave()) {

                // todo 建议从从服务器拉取消息，则使用消费组订阅配置中的 whichBrokerWhenConsumeSlowly，默认为 1，也就是从从服务器
                // 可以通过客户端命令 updateSubGroup 指定当主服务器繁忙时，建议从哪个从服务器读取消息
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());

                // 不建议从 slave 拉取消息，则设置 0 ，表示从主服务器拉取消息
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            // 主从同步相关
            switch (this.brokerController.getMessageStoreConfig().getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    break;

                // 如果当前服务器的角色为从服务器:并且slaveReadEnable=false，则忽略上面代码设置的值，下次拉取切换为从主拉取。
                case SLAVE:
                    if (!this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        // 建议下次从主服务器拉取
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    }
                    break;
            }

            // 如果从允许读
            if (this.brokerController.getBrokerConfig().isSlaveReadEnable()) {

                // 如果建议从从服务器读取，说明消息消费缓慢
                // consume too slow ,redirect to another machine
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    // 则使用消费组订阅配置中的 whichBrokerWhenConsumeSlowly，默认为 1，也就是从从服务器
                    // todo 顺便说下，whichBrokerWhenConsumeSlowly 属性值 是消费组订阅配置指定在消费缓慢时建议的拉取 brokerId
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                }

                // 不建议从从服务器读取，也就是说消息消费速度正常，则使用订阅组建议的brokerId拉取消息进行消费，默认为主服务器
                // consume ok
                else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }

                // 如果从不允许读，那么下次拉取消息只能从主服务器
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            // todo 根据查询到的消息状态设置 响应状态
            switch (getMessageResult.getStatus()) {
                // 成功
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                // 消息存放在下一个 commitLog 中
                case MESSAGE_WAS_REMOVING:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY); // 立即重试拉取消息
                    break;
                // 未找到队列
                case NO_MATCHED_LOGIC_QUEUE:

                case NO_MESSAGE_IN_QUEUE:
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED); // 拉取偏移量变动
                        // XXX: warn and notify me
                        log.info("the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
                                requestHeader.getQueueOffset(),
                                getMessageResult.getNextBeginOffset(),
                                requestHeader.getTopic(),
                                requestHeader.getQueueId(),
                                requestHeader.getConsumerGroup()
                        );
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;

                // 队列中未包含消息，在 Broker 端过滤后没有消息了
                case NO_MATCHED_MESSAGE:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY); // 立即重试拉取消息
                    break;

                // 没有对应的消息索引
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                //offset越界
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}",
                            requestHeader.getQueueOffset(), getMessageResult.getMaxOffset(), channel.remoteAddress());
                    break;
                //offset在队列中未找到
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                //offset未在队列中
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                            getMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            // hook
            if (this.hasConsumeMessageHook()) {
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(requestHeader.getConsumerGroup());
                context.setTopic(requestHeader.getTopic());
                context.setQueueId(requestHeader.getQueueId());

                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                        int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        break;
                    case ResponseCode.PULL_NOT_FOUND:
                        // 消息未查询到 && broker允许挂起请求 && 请求允许挂起
                        if (!brokerAllowSuspend) {

                            context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);

                        }
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    case ResponseCode.PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);
                        break;
                    default:
                        assert false;
                        break;
                }

                this.executeConsumeMessageHookBefore(context);
            }


            // 根据映射的响应码处理
            switch (response.getCode()) {

                // 拉取到消息
                case ResponseCode.SUCCESS:
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());

                    // 读取消息

                    // 默认把拉取的消息拷贝到堆内存
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) { // 内存中
                        final long beginTimeMills = this.brokerController.getMessageStore().now();
                        // 获取消息内容到堆内存，设置到响应 Body
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                                requestHeader.getTopic(), requestHeader.getQueueId(),
                                (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        response.setBody(r);

                        // 零拷贝，不经过堆
                    } else { // zero-copy
                        try {

                            // 基于 zero-copy 实现，直接响应，无需堆内内存，性能更优
                            FileRegion fileRegion =
                                    new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);

                            // 通过 mappedBuffer 发送到 SocketBuffer
                            // todo 即拷贝到 Socket Buffer
                            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    getMessageResult.release();
                                    if (!future.isSuccess()) {
                                        log.error("transfer many message by pagecache failed, {}", channel.remoteAddress(), future.cause());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            getMessageResult.release();
                        }

                        response = null;
                    }
                    break;

                /**
                 * 消息拉取长轮询机制分析 - 所谓轮询就是不停地判断拉取消息的请求的逻辑偏移量和消息队列最大逻辑偏移量的大小关系，大于消息消费队列的最大偏移量时才挂起，一旦检测发现待拉取消息偏移量小于消费队列最大偏移量时，则尝试拉取消息，结束长轮询过程。
                 *
                 * 1 RocketMQ未真正实现消息推模式，而是消费者主动向消息服务器拉取消息，RocketMQ推模式是循环向消息服务端发起消息拉取请求
                 * 2 消息未到达消费队列时，如果不启用长轮询机制，则会在服务端等待shortPollingTimeMills时间后再去判断消息是否已经到达指定消息队列，
                 *   如果消息仍未到达则提示拉取消息客户端PULL—NOT—FOUND（消息不存在）；
                 * 3 如果开启长轮询模式，RocketMQ一方面会每隔5s轮询检查一次消息是否可达，同时一有消息达到后立马通知挂起线程再次验证消息是否是自己感兴趣的消息，
                 *   如果是则从CommitLog文件中提取消息返回给消息拉取客户端，否则直到挂起超时
                 * 4 长轮询和端轮询的区别是：长轮询等待时间更长，短轮询等待时间非常短，如 1s，可以看作连接建立/断开都很频繁
                 *
                 * 在拉取消息时，如果拉取结果为 PULL_NOT_FOUND，在服务端默认会挂起线程，然后根据是否启用长轮询机制，延迟一定时间后再次重试根据偏移量查找消息。
                 */
                // 消息未查询到 && broker 允许挂起请求 && 请求允许挂起，则执行挂起请求
                case ResponseCode.PULL_NOT_FOUND:

                    // 允许 Broker 挂起 && 没有消息时挂起请求一段时间
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        // 取挂起多久时间
                        long pollingTimeMills = suspendTimeoutMillisLong;

                        // 没有开启长轮询，就使用短轮询时间
                        // 默认是开启的
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            // 默认为1000ms作为下一次拉取消息的等待时间。
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        String topic = requestHeader.getTopic();
                        long offset = requestHeader.getQueueOffset();
                        int queueId = requestHeader.getQueueId();

                        // 创建拉取请求 - 长轮询包下的
                        // 用于保存当前拉取消息的请求，后续使用定期线程扫描该请求，判断是否有消息到达
                        PullRequest pullRequest = new PullRequest(
                                request,
                                channel,
                                pollingTimeMills,
                                this.brokerController.getMessageStore().now(),
                                offset, // 待拉取消息的逻辑偏移量
                                subscriptionData, // 订阅信息
                                messageFilter // 消息过滤器，在获取消息前，快速过滤无效消息索引
                        );

                        // todo 添加到挂起请求队列，等待后续重新拉取
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);

                        // todo 设置response=null，则此时此次调用不会向客户端输出任何字节，客户端网络请求客户端的读事件不会触发，不会触发对响应结果的处理，处于等待状态。
                        // todo 这就是所谓的挂起请求
                        response = null;
                        break;
                    }

                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                            || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(requestHeader.getQueueId());
                        mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(requestHeader.getConsumerGroup());
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(requestHeader.getQueueOffset());
                        event.setOffsetNew(getMessageResult.getNextBeginOffset());
                        this.generateOffsetMovedEvent(event);
                        log.warn(
                                "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                                requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(),
                                responseHeader.getSuggestWhichBrokerId());

                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(),
                                responseHeader.getSuggestWhichBrokerId());
                    }

                    break;
                default:
                    assert false;
            }

            // 拉取结果为空
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }

        // todo 如果消费端提交消费进度,并且当前Broker为主节点,则更新消息消费进度
        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        storeOffsetEnable = storeOffsetEnable
                && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;

        // todo 消费端提交消费进度过来了
        if (storeOffsetEnable) {
            // 提交消费进度
            this.brokerController.getConsumerOffsetManager().commitOffset(
                    RemotingHelper.parseChannelRemoteAddr(channel),
                    requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(),
                    requestHeader.getQueueId(),
                    requestHeader.getCommitOffset()
            );
        }
        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
                                        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
//                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
//                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                        + 4 // 2 MAGICCODE
                        + 4 // 3 BODYCRC
                        + 4 // 4 QUEUEID
                        + 4 // 5 FLAG
                        + 8 // 6 QUEUEOFFSET
                        + 8 // 7 PHYSICALOFFSET
                        + 4 // 8 SYSFLAG
                        + 8 // 9 BORNTIMESTAMP
                        + bornhostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    /**
     * 进行消息拉取，结束长轮询。并返回结果到客户端。
     *
     * @param channel 连接通道
     * @param request 请求命令
     * @throws RemotingCommandException
     */
    public void executeRequestWhenWakeup(final Channel channel,
                                         final RemotingCommand request) throws RemotingCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {

                    // 1 执行拉取请求。本次调用，设置了即使请求不到消息，也不挂起请求。如果不设置，请求可能被无限挂起，被 Broker 无限循环。
                    // todo 最后一个参数是 false ，表示 Broker 端不支持挂起
                    final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

                    // 2 将拉取结果响应给消费客户端
                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {

                            // 通过通道将消息写回消费客户端
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {

                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to {} failed",
                                                future.channel().remoteAddress(), future.cause());
                                        log.error(request.toString());
                                        log.error(response.toString());
                                    }
                                }
                            });

                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };

        // 提交拉取消息请求到线程池
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }
}
