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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 集群模式消费进度存储，即 Consumer 消费进度管理，负责从 Broker 获取消费进度，同步消费进度到 Broker。
 * - 获取某个 MessageQueue 的消费进度，可以从内存中获取，也可以从 Broker 获取（获取后更新客户端缓存）
 * - 定期将本地缓存的消费进度上报到 Broker ，Broker 会每 5s 将消息消费偏移量持久化到磁盘文件。
 * <p>
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * MQ 客户端实例，该实例被同一个客户端的消费者、生产者共用
     */
    private final MQClientInstance mQClientFactory;

    /**
     * MQ 消费组
     */
    private final String groupName;

    /**
     * 内存中的消费进度
     * todo 说明：是针对消息队列的逻辑偏移量
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    /**
     * 集群模式下，不需要加载。实际读取消费进度时，从 Broker 获取
     */
    @Override
    public void load() {
    }

    /**
     * 更新消息队列消费进度 （内存中的）
     *
     * @param mq
     * @param offset
     * @param increaseOnly
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            // 获取传入的消息队列的消费进度
            AtomicLong offsetOld = this.offsetTable.get(mq);

            // 没有的话，则缓存
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            // 如果不为空，说明同时对一个MQ消费队列进行消费，并发执行
            if (null != offsetOld) {

                // 根据 increaseOnly 更新原先的 offsetOld 的值
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * 读取消费队列的消费进度
     *
     * @param mq
     * @param type
     * @return
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                // 先从内存中读取
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                // 从磁盘读取
                case READ_FROM_STORE: {
                    try {
                        // 从 Broker 读取消费进度
                        // todo 代码逻辑是逻辑偏移量
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);

                        // todo 以 Broker 消费进度为准
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * 持久化指定消息队列数组的消费进度到 Broker，并移除非指定消息队列
     *
     * @param mqs
     * @see MQClientInstance#startScheduledTask() // 客户端启动定时线程
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        // 要持久化消费进度的消息队列
        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();

        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            AtomicLong offset = entry.getValue();
            if (offset != null) {
                if (mqs.contains(mq)) {
                    try {
                        // RPC 更新消费进度
                        this.updateConsumeOffsetToBroker(mq, offset.get());
                        log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                    } catch (Exception e) {
                        log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                    }
                } else {
                    unusedMQ.add(mq);
                }
            }
        }

        // 移除不适用的消息队列
        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                        this.groupName,
                        this.mQClientFactory.getClientId(),
                        mq,
                        offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                    offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * 更新消费进度到Broker
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            // 组装好更新消费进度的 RPC 请求头
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    /**
     * 从磁盘读取消费进度。
     * 消费进度是保存在 broker 哪个地方：
     * Broker端的offset管理参照 ConsumerOffsetManager，保存逻辑其实与广播模式差不多，offset保存的路径：/rocketmq_home/store/config/consumerOffset.json。
     *
     * @param mq
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {

        // 根据消息队列中的 broker 的名称获取 broker 地址，然后发送请求
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {

            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());

            // 从 Broker 拉取执行 Topic 的某个消费组下的某个队列的消费进度
            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
