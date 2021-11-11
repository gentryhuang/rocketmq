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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤器
 */
public interface MessageFilter {

    /**
     * 根据 ConsumeQueue 判断消息是否匹配。
     * todo 基于 TAG 模式根据 ConsumeQueue 进行消息过滤时只对比 tag 的哈希码
     * <p>
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     *
     * @param tagsCode  tagsCode 消息标志的 哈希码
     * @param cqExtUnit extend unit of consume queue ConsumeQueue 条目扩展属性
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
                                    final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * 根据存储在 CommitLog 文件中的内存判断消息是否匹配
     * todo 主要用于 SQL92 表达式模式服务的，根据消息属性实现类似数据库 SQL where 条件的过滤方式
     * <p>
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *
     * @param msgBuffer  message buffer in commit log, may be null if not invoked in store. 消息内从，如果为空，该方法返回 true
     * @param properties message properties, should decode from buffer if null by yourself.
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
                                 final Map<String, String> properties);
}
