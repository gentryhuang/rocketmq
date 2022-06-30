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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤器默认实现
 */
public class DefaultMessageFilter implements MessageFilter {

    private SubscriptionData subscriptionData;

    public DefaultMessageFilter(final SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    /**
     * 根据 ConsumeQueue 判断消息是否匹配
     *
     * @param tagsCode  tagsCode 消息标志的 哈希码
     * @param cqExtUnit extend unit of consume queue ConsumeQueue 条目扩展属性
     * @return
     */
    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (null == tagsCode || null == subscriptionData) {
            return true;
        }

        // 类过滤的话，那么 Tag 过滤就不生效了
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // 如果订阅数据中的订阅表达式为 * ，或者订阅数据中的 Tag 的哈希码包含当前消息索引中的 tag 的哈希码，说明是匹配的
        return subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)
                || subscriptionData.getCodeSet().contains(tagsCode.intValue());
    }

    /**
     * 根据存储在 CommitLog 文件中的内存判断消息是否匹配
     *
     * @param msgBuffer  message buffer in commit log, may be null if not invoked in store. 消息内从，如果为空，该方法返回 true
     * @param properties message properties, should decode from buffer if null by yourself.
     * @return
     */
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        return true;
    }
}
