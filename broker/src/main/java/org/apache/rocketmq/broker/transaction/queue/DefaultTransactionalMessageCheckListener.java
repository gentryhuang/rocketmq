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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * 事务消息回查回调实现
 */
public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it by moving it to system topic TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            // 封装 Broker 内部消息对象，用于将回查次数达到阈值（默认 15次）的 half 消息放入到 TRANS_CHECK_MAX_TIME_TOPIC 主题下
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            // 将消息追加到 CommitLog 中
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner);

            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, " +
                        "commitLogOffset={}, real topic={}", msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            } else {
                log.error("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e);
        }

    }

    /**
     * 将回查达到阈值或过期的半消息存入到 Topic 为 RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC 中
     *
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        // 主题：TRANS_CHECK_MAX_TIME_TOPIC
        // queueId:
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager().createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);
        int queueId = Math.abs(random.nextInt() % 99999999) % TCMT_QUEUE_NUMS;

        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        // 指定 Topic 为 TRANS_CHECK_MAX_TIME_TOPIC
        inner.setTopic(topicConfig.getTopicName());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(queueId);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
