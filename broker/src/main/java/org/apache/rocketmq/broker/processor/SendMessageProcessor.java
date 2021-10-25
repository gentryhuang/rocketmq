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

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * Broker端 处理发送请求类
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    /**
     * 处理消息生产者发送的消息请求
     *
     * @param ctx     Netty ctx
     * @param request 请求对象
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = null;
        try {
            // 同步处理生产者发来的请求
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request.toString(), e);
        }
        return response;
    }

    @Override
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        // 异步处理请求
        asyncProcessRequest(ctx, request).thenAcceptAsync(responseCallback::callback, this.brokerController.getSendMessageExecutor());
    }

    /**
     * 异步处理请求
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx,
                                                                  RemotingCommand request) throws RemotingCommandException {
        final SendMessageContext mqtraceContext;
        // 根据请求码，确定是什么请求
        switch (request.getCode()) {
            // 消费重试请求
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.asyncConsumerSendMsgBack(ctx, request);

            // 非重试请求
            default:
                // 解析请求
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return CompletableFuture.completedFuture(null);
                }

                // 构建消息内容
                mqtraceContext = buildMsgContext(ctx, requestHeader);

                // hook：处理发送消息前逻辑
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

                // 批量发送消息逻辑
                if (requestHeader.isBatch()) {
                    return this.asyncSendBatchMessage(ctx, request, mqtraceContext, requestHeader);

                    // 非批量发送消息
                    // todo 包含对事务消息的处理
                } else {
                    return this.asyncSendMessage(ctx, request, mqtraceContext, requestHeader);
                }
        }
    }

    @Override
    public boolean rejectRequest() {
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
                this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    /**
     * 处理 Consumer 的重试请求
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx,
                                                                        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            ConsumeMessageContext context = buildConsumeMessageContext(namespace, requestHeader, request);
            this.executeConsumeMessageHookAfter(context);
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return CompletableFuture.completedFuture(response);
        }
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return CompletableFuture.completedFuture(response);
        }

        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return CompletableFuture.completedFuture(response);
        }

        // 1 更新消息的 Topic 为 %RETRY% + group
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());

        // 计算 queueId （重试队列，队列数为 1）
        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                newTopic,
                subscriptionGroupConfig.getRetryQueueNums(),
                PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return CompletableFuture.completedFuture(response);
        }

        // 队列是否有写权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return CompletableFuture.completedFuture(response);
        }

        // 根据偏移量从 CommitLog 中取消息
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return CompletableFuture.completedFuture(response);
        }

        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        // 获取延迟级别，目前都是 0
        // 后续会根据重试次数，调整延迟级别
        int delayLevel = requestHeader.getDelayLevel();

        // 获取配置的消息重试次数
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
        }

        // 如果消息重试次数 >= 16(默认)。更新消息的 Topic 为死信队列的 Topic ："%DLQ%" + group ，消费队列为 1（死信队列只有一个消费队列）
        // todo 死信：以"%DLQ%" + group为Topic名，存到CommitLog中：存到死信队列中的消息不会被Consumer消费了
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {

            // 更改 Topic 名为死信队列名，"%DLQ%" + group
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());

            // 默认死信队列数为1
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE, 0);
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return CompletableFuture.completedFuture(response);
            }

            /**
             * todo 重试-延迟
             * 1 如果没有变成死信，计算消息的延迟级别，作为一个新的延迟消息存到 Commitlog 中，当该消息到了消费时间点是会被 Consumer 重新消费的。
             * 2 该消息以新的Topic名："%RETRY%"+ group 存到CommitLog中作为延迟消息
             * todo 延迟时间：
             * 1 重试消息发到Broker后，被作为一个新的延迟消息存到了CommitLog中，当该消息到了消费时间点是会被Consumer重新消费的。
             * 2 消息重试16次才会被丢到 死信队列中，才不会被消费,那其余15次消息每次延迟是延迟多久呢？
             * 3 消息的延迟级别是受重试次数（reconsumeTimes）影响的。重试次数越大，延迟越久。
             *
             */
        } else {

            // delayLevel 其实都为0，所以这里就相当于是重试次数 +3
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }
            msgExt.setDelayTimeLevel(delayLevel);
        }

        // 复制原来消息，重新生成一个 Msg ，将新消息丢给 BrokerController 中，然后存储到 CommitLog 中进行存储
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());

        // todo 消息重试次数 + 1
        // 新消息被消费者消费时就会传上来
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        // 作为新消息存到CommitLog中
        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        return putMessageResult.thenApply((r) -> {
            if (r != null) {
                switch (r.getPutMessageStatus()) {
                    case PUT_OK:
                        String backTopic = msgExt.getTopic();
                        String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        if (correctTopic != null) {
                            backTopic = correctTopic;
                        }
                        this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);
                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                        return response;
                    default:
                        break;
                }
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(r.getPutMessageStatus().name());
                return response;
            }
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
            return response;
        });
    }


    /**
     * 处理发送来的消息-发送消息
     *
     * @param ctx
     * @param request
     * @param mqtraceContext
     * @param requestHeader
     * @return
     */
    private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx,
                                                                RemotingCommand request,
                                                                SendMessageContext mqtraceContext,
                                                                SendMessageRequestHeader requestHeader) {
        // 初始化响应
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        // 获取请求内容
        final byte[] body = request.getBody();

        // 获取要发送的队列id
        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        // 如果队列id小于0，从可用队列随机选择
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        // 创建MessageExtBrokerInner
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        // 处理重试和死信队列的情况
        // 如果消息达到最大重试次数，默认 16次，就放入到死信队列
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return CompletableFuture.completedFuture(response);
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        CompletableFuture<PutMessageResult> putMessageResult = null;

        // 解析请求中的属性集合
        Map<String, String> origProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());

        // 判断是否是事务消息
        // todo 如果是事务消息，在发送消息的时候会进行打标 TRAN_MSG
        String transFlag = origProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);

        // 是事务消息
        if (transFlag != null && Boolean.parseBoolean(transFlag)) {

            // 校验是否不允许发送事务消息，默认允许
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return CompletableFuture.completedFuture(response);
            }

            // 存储事务消息
            putMessageResult = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);

            // 存储普通消息
        } else {
            putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        }

        // 处理存储消息结果
        return handlePutMessageResultFuture(putMessageResult, response, request, msgInner, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    private CompletableFuture<RemotingCommand> handlePutMessageResultFuture(CompletableFuture<PutMessageResult> putMessageResult,
                                                                            RemotingCommand response,
                                                                            RemotingCommand request,
                                                                            MessageExt msgInner,
                                                                            SendMessageResponseHeader responseHeader,
                                                                            SendMessageContext sendMessageContext,
                                                                            ChannelHandlerContext ctx,
                                                                            int queueIdInt) {
        return putMessageResult.thenApply((r) ->
                handlePutMessageResult(r, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt)
        );
    }

    /**
     * 处理重试和死信队列
     *
     * @param requestHeader
     * @param response
     * @param request
     * @param msg
     * @param topicConfig
     * @return
     */
    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
                                      RemotingCommand request,
                                      MessageExt msg, TopicConfig topicConfig) {

        // 对RETRY类型的消息处理。如果超过最大消费次数，则topic修改成"%DLQ%" + 分组名，即加入 死信队列(Dead Letter Queue)
        String newTopic = requestHeader.getTopic();

        // 当前消息是重试消息
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            // 获取订阅分组，因为重试主题格式：%RETRY%+消费组
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());

            // 获取订阅分组
            SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark(
                        "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            // 计算最大可消费次数，最大 16 次
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }

            // 获取请求重试次数
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();

            // 超过最大消费次数，加入到死信对列
            if (reconsumeTimes >= maxReconsumeTimes) {
                // 死信队列主题：%DLQ%+groupName
                newTopic = MixAll.getDLQTopic(groupName);
                // 死信队列id
                int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                        DLQ_NUMS_PER_GROUP,
                        PermName.PERM_WRITE, 0
                );
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);

                // 死信队列，无需再处理消息
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    /**
     * 发送消息，并返回发送消息结果。
     *
     * @param ctx
     * @param request
     * @param sendMessageContext
     * @param requestHeader
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext sendMessageContext,
                                        final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

        // 初始化响应
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendMessage request command, {}", request);

        // 如果未开始接收消息，跑出系统异常
        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        // 消息配置(Topic配置）校验
        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        // 如果队列小于0，从可用队列随机选择
        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        // 创建MessageExtBrokerInner
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        // 死信队列处理
        // 对RETRY类型的消息处理。如果超过最大消费次数，则topic修改成”%DLQ%” + 分组名， 即加 死信队 (Dead Letter Queue)
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return response;
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = null;
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());

        // 获取是否事务消息
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);

        // 消息存储
        if (traFlag != null && Boolean.parseBoolean(traFlag)
                && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1

            // 校验是否不允许发送事务消息
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return response;
            }

            // 存储事务消息
            putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);

            // 存储普通消息
        } else {

            // 消息存储，即使用 MessageStore 存储消息，调用 putMessage 方法
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }

        // 响应结果
        return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);

    }

    /**
     * 响应消息发送的结果
     *
     * @param putMessageResult   添加消息后的返回结果
     * @param response
     * @param request
     * @param msg
     * @param responseHeader
     * @param sendMessageContext
     * @param ctx
     * @param queueIdInt
     * @return
     */
    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        // 根据写入消息后的结果状态值，设置响应状态
        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                        "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now. It may be caused by one of the following reasons: " +
                                "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {

            // 统计
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            // 响应
            response.setRemark(null);
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            doResponse(ctx, request, response);

            // hook：设置发送成功到context
            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            return null;
        } else {
            // hook：设置发送失败到context
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    private CompletableFuture<RemotingCommand> asyncSendBatchMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                     SendMessageContext mqtraceContext,
                                                                     SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return CompletableFuture.completedFuture(response);
        }

        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
        return handlePutMessageResultFuture(putMessageResult, response, request, messageExtBatch, responseHeader, mqtraceContext, ctx, queueIdInt);
    }


    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis =
                StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
                StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    static private ConsumeMessageContext buildConsumeMessageContext(String namespace,
                                                                    ConsumerSendMsgBackRequestHeader requestHeader,
                                                                    RemotingCommand request) {
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setNamespace(namespace);
        context.setConsumerGroup(requestHeader.getGroup());
        context.setTopic(requestHeader.getOriginTopic());
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));
        return context;
    }

    private int randomQueueId(int writeQueueNums) {
        return (this.random.nextInt() % 99999999) % writeQueueNums;
    }

    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request,
                                    SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);

        response.setOpaque(request.getOpaque());
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);

        final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();

        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        return response;
    }
}
