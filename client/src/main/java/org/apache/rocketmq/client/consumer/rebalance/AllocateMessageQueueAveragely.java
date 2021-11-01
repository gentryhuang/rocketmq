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

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 队列分配策略 - 平均分配
 * 如果队列数和消费者数量相除有余数时，余数按照顺序 1 个 1个 分配消费者。
 * 举例:8个队列q1,q2,q3,q4,q5,a6,q7,q8,消费者3个:c1,c2,c3
 * 分配如下:
 * c1:q1,q2,q3
 * c2:q4,q5,a6
 * c3:q7,q8
 * todo 特别描述：
 *  平均分配算法，类似于分页的算法，将所有MessageQueue排好序类似于记录，将所有消费端Consumer排好序类似页数，并求出每一页需要包含的平均size和每个页面记录的范围range，
 *  最后遍历整个range而计算出当前Consumer端应该分配到的记录（这里即为：MessageQueue）。
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 平均分配队列策略
     *
     * @param consumerGroup 消费组
     * @param currentCID    要分配队列的消费者id
     * @param mqAll         Topic 下的队列
     * @param cidAll        消费者集合
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {
        // 检验参数是否正确
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        // 分配后的消息队列
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);
            return result;
        }

        // 平均分配队列

        // 当前 Consumer 在消费集群中是第几个 consumer
        // 这里就是为什么需要对传入的 cidAll 参数必须进行排序的原因。如果不排序，Consumer 在本地计算出来的 index 无法一致，影响计算结果。
        int index = cidAll.indexOf(currentCID);


        // 余数，即多少消息队列无法平均分配
        int mod = mqAll.size() % cidAll.size();

        // 队列总数 <= 消费者总数时，分配当前消费则1个队列
        // 不能均分 && 当前消费者序号（从 0 开始） < 余下的队列数，分配当前消费者 mqAll /cidAll + 1 个队列
        // 不能均分 && 当前消费者序号（从 0 开始） >= 余下的队列数，分配当前消费者 mqAll/cidAll 个队列
        int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());

        // 有余数的情况下，[0, mod) 平分余数，即每consumer多分配一个节点；第index开始，跳过前mod余数
        // Consumer 分配消息队列开始位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        // 分配队列数量。之所以要Math.min()的原因是，mqAll.size() <= cidAll.size()，部分consumer分配不到消息队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        // 分配消息队列结果
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
