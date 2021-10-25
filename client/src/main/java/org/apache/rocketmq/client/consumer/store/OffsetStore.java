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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费进度存储
 * - 用来管理当前 OffsetStore 对应的消费端的每个消费队列的不同消费组的消费进度。
 * 广播模式下：
 * 同消费组的消费者相互独立，消费进度要单独存储；
 *
 * 集群模式下：
 * 同一条消息只会被同一个消费组消费一次，消费进度和负载均衡相关，因此消费进度需要共享
 * - Broker 管理的消费队列的偏移量，是针对某个消费组的Topic 下每个消费队列的进度
 * - 当前 OffsetStore 对应的消费端的进度会定时上报到 Broker
 * <p>
 * 对offset的管理分为本地模式和远程模式，本地模式是以文本文件的形式存储在客户端，而远程模式是将数据保存到broker端，
 * 对应的数据结构分别为LocalFileOffsetStore和RemoteBrokerOffsetStore。
 * 1 默认情况下，当消费模式为广播模式时，offset使用本地模式存储，因为每条消息会被所有的消费者消费，每个消费者管理自己的消费进度，各个消费者之间不存在消费进度的交集；
 * 2 当消费模式为集群消费时，则使用远程模式管理offset，消息会被多个消费者消费，不同的是每个消费者只负责消费其中部分消费队列，添加或删除消费者，都会使负载发生变动，容易造成消费进度冲突，因此需要集中管理。
 * <p>
 * <p>
 * 说明：
 * 1 RemoteBrokerOffsetStore: Consumer 集群模式下，使用远程 Broker 消费进度
 * 2 LocalFileOffsetStore: Consumer 广播模式下，使用本地文件消费进度
 * 3 todo 消费进度持久化不仅仅只有定时持久化，拉取消息、分配消息队列等等操作，都会进行消费进度持久化。
 */
public interface OffsetStore {
    /**
     * 加载消费进度到内存
     */
    void load() throws MQClientException;

    /**
     * 更新内存中的消息进度
     * Update the offset,store it in memory
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * Get offset from local storage
     * 读取消费进度类型：
     * - READ_FROM_MEMORY：从内存读取
     * - READ_FROM_STORE：从存储( Broker 或 文件 )读取
     * - MEMORY_FIRST_THEN_STORE ：优先从内存读取，读取不到，从存储读取
     *
     * @return The fetched offset
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化消费进度
     * Persist all offsets,may be in local storage or remote name server
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * Remove offset
     */
    void removeOffset(MessageQueue mq);

    /**
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 更新存储在 Broker 端的消息消费进度，使用集群模式
     *
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;
}
