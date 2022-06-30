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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 根据 Broker 部署机房名，对每个消费者负载不同的 Broker 上的队列。
 * 即 根据指定的 Broker 名，从队列中选出属于这些 Broker 的队列"平均"分配给消费者
 * <p>
 * Computer room Hashing queue algorithm, such as Alipay logic room
 */
public class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy {
    /**
     * 消费者消费 brokerName 集合
     */
    private Set<String> consumeridcs;

    /**
     * @param consumerGroup 消费组
     * @param currentCID    消费者id
     * @param mqAll         Topic 下的队列
     * @param cidAll        消费者集合
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup,
                                       String currentCID,
                                       List<MessageQueue> mqAll,
                                       List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }

        // 计算可消费的 Broker 对应的消息队列，即当前配置的消费者数组('consumeridcs')对应的消息队列
        List<MessageQueue> premqAll = new ArrayList<MessageQueue>();
        for (MessageQueue mq : mqAll) {
            String[] temp = mq.getBrokerName().split("@");
            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
                premqAll.add(mq);
            }
        }

        // 平均分配
        // 该平均分配方式和 AllocateMessageQueueAveragely 略有不同，其是将多余的结尾部分分配给前 rem 个 Consumer。
        int mod = premqAll.size() / cidAll.size();
        int rem = premqAll.size() % cidAll.size();
        int startIndex = mod * currentIndex;
        int endIndex = startIndex + mod;
        for (int i = startIndex; i < endIndex; i++) {
            result.add(premqAll.get(i));
        }
        if (rem > currentIndex) {
            result.add(premqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM";
    }

    public Set<String> getConsumeridcs() {
        return consumeridcs;
    }

    public void setConsumeridcs(Set<String> consumeridcs) {
        this.consumeridcs = consumeridcs;
    }
}
