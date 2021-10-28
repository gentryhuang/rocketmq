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

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

/**
 * 消费处理服务，消费者会根据不同的消费类型（消费监听器）在启动时创建。用来消费从 Broker 拉取的消息
 * <p>
 * 消息消费逻辑（非顺序消息），不断消费消息，并处理消费结果
 * 1 将待消费的消息存入 ProcessQueue 中，并执行消息消费之前钩子函数
 * 2 修改待消费消息的主题（设置为消费组的重试主题）
 * 3 分页消费
 */
public interface ConsumeMessageService {
    void start();

    void shutdown(long awaitTerminateMillis);

    void updateCorePoolSize(int corePoolSize);

    void incCorePoolSize();

    void decCorePoolSize();

    int getCorePoolSize();

    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 提交消费消息请求到线程池
     *
     * @param msgs             从 Broker 拉取的消息（Broker 通过 MessageStore 存储服务）
     * @param processQueue     消息处理队列
     * @param messageQueue     消息队列
     * @param dispathToConsume 派发给消费者消费
     */
    void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume);
}
