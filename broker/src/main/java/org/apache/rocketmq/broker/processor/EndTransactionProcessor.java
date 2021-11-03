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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * Broker 处理结束事务请求
 * 说明：
 * 1
 * <p>
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 处理事务结束请求
     * 注意：对于本地事务执行未知状态，这里不做处理。只会对提交和回滚进行处理。
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
            RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        // 解码出事务请求
        final EndTransactionRequestHeader requestHeader =
                (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.debug("Transaction request:{}", requestHeader);
        // 从节点不处理
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }

        /*----------------------------- 判断是否处理和打印日志 ---------------*/

        // 事务检查，默认不检查
        if (requestHeader.getFromTransactionCheck()) {
            // 事务状态
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());

                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            // 事务状态
            switch (requestHeader.getCommitOrRollback()) {
                // 未知
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }

                // 已提交
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }

                // 回滚
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }

        /*----------------------------- 判断是否处理和打印日志 ---------------*/


        /**--------------- 走到这里，说明是提交或者回滚事务请求。会将消息存储到 Op 主题下，标识对应的半事务消息被处理了 ------------------*/

        // 操作结果
        OperationResult result = new OperationResult();

        // 如果请求是提交事务，进入事务消息提交处理流程
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {

            // 根据之前提交的半消息的 offset 从 CommitLog 文件中查找消息
            // requestHeader 中包含 commitLogOffset
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);

            if (result.getResponseCode() == ResponseCode.SUCCESS) {

                // 字段检查：检查下这条信息是否正确，消息偏移量、生产者组什么的是否匹配，是不是查错消息了
                // result.getPrepareMessage() 就是之前存储到 Broker 的半消息。
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);

                // 根据备份信息重新构造消息并投递
                if (res.getCode() == ResponseCode.SUCCESS) {

                    // 恢复事务消息的真实的主题、队列、并设置事务ID
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());

                    //设置消息的相关属性，取消事务相关的系统标记
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));

                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());

                    // 清除事务消息属性 TRAN_MSG ,这样就是一个普通消息了
                    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);

                    // todo 原始消息写入对应的topic，此时对消费端就可见了，可以正常消费了
                    RemotingCommand sendResult = sendFinalMessage(msgInner);

                    // 事务消息发送成功后
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {

                        // 删除预处理消息(prepare)
                        // 其实是将消息存储在主题为 RMQ_SYS_TRANS_OP_HALF_TOPIC 的主题中，消息 tag 是 d，消息体是半消息的逻辑偏移量；代表这些消息已经被处理（提交或回滚）。
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }


            // 回滚处理
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {

            // 根据 offset 从 CommitLog 中查找消息
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                //字段检查
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);

                // 注意，回滚只是做标记
                if (res.getCode() == ResponseCode.SUCCESS) {
                    //删除预处理消息(prepare)
                    //将消息存储在RMQ_SYS_TRANS_OP_HALF_TOPIC中，代表该消息已被处理
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }

        /**
         * 总结一下：
         * commit：就是先把原始消息写入到原始topic里，然后记录 op 消息对半消息打标，表示事务状态已经确定
         * rollback: 就是直接记录 op 消息对半消息打标，表示事务状态已经确定
         * unknown: 就是上面 两步都没做，原始消息未写入，op队列里也没有
         */

        // UNKNOW 状态，看来得回查了，其实这里返不返回都一样，客户端是one way调用
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }

            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }

            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * 构建事务消息
     * 说明：恢复事务消息的真实的主题、队列，并设置事务ID
     *
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        // 恢复事务消息的真实的主题、队列
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
                (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                        : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        // 清除掉消息中的原 Topic 和 queueId 属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    /**
     * 存储最终的确认消息
     *
     * @param msgInner
     * @return
     */
    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);

        // 处理存储结果
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
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
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
