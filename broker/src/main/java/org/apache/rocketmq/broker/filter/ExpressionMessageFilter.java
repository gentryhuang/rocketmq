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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 表达式模式过滤器
 */
public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    /**
     * 基于消费端拉取消息请求构建的 订阅数据信息
     */
    protected final SubscriptionData subscriptionData;
    protected final ConsumerFilterData consumerFilterData;
    protected final ConsumerFilterManager consumerFilterManager;
    protected final boolean bloomDataValid;

    /**
     * @param subscriptionData      订阅表达式转换的订阅数据
     * @param consumerFilterData    SQL92 过滤对象
     * @param consumerFilterManager
     */
    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
                                   ConsumerFilterManager consumerFilterManager) {

        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        // 如果 subscriptionData 为空，说明此模式不是 Expression模式，直接返回true
        if (null == subscriptionData) {
            return true;
        }

        // 如果 classFilterMode, 直接返回 true，这里也表明，isMatchedByConsumeQueue 不处理class filter mode
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // 如果是 TAG 模式，只需要比对 tag 的 hashcode, 因为 consumequeue 只包含了tag hashcode
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {

            if (tagsCode == null) {
                return true;
            }

            // 如果订阅表达式为 * ，那么就是全匹配
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            // 比对 tag 的 hashcode
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());

        } else {
            // no expression or no bloom
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                    || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }

            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                    || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                        + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    /**
     * isMatchedByCommitLog 只为 ExpressionType.SQL92 服务；SQL92 是基于 SQL 表达式，根据存储在 CommitLog 文件中的内存判断消息是否匹配
     *
     * @param msgBuffer  message buffer in commit log, may be null if not invoked in store. 如果为空，该方法返回 true
     * @param properties message properties, should decode from buffer if null by yourself.
     * @return
     */
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        // 如果 subscriptionData 为空，说明此模式是 classFilter，直接返回true
        if (subscriptionData == null) {
            return true;
        }

        // 如果是类过滤模式，则直接返回 true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // 如果是 Tag ，则直接返回 true
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
                || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        // 从消息体中解码出属性
        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            // 使用过滤数据对象对消息中的属性进行匹配
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }

}
