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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * RocketMQ 消息发送容错策略，延时实现的门面类
 * todo 特别说明：
 * 1 开启与不开启延迟规避机制，在消息发送时都能在不同程度上规避故障的 Broker
 * 2 区别：
 * 2.1 开启故障延迟机制，其实是一种悲观的做法。一旦消息发送失败后就会悲观认为 Broker 不可用，把这个 Broker 记录下来，在接下来的一段时间（延迟规避时间）内就不能再向其发送消息，直接避开该 Broker。
 * 2.2 未开始故障延迟机制，只会在本地消息发送的重试过程中规避该 Broker，下一次消息发送是个无状态，还是会继续尝试
 * 3 联系：开始故障延迟机制是将认为出现问题的 Broker 记录下来，指定多长时间内不能参与消息队列负载；不开启故障延迟机制，只是对上次出现的故障进行规避；
 * <p>
 * 默认情况下容错策略关闭，即 sendLatencyFaultEnable=false
 */
public class MQFaultStrategy {

    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * 延迟故障容错，维护每个 Broker 的发送消息的延迟
     * todo 说明：latencyFaultTolerance机制是实现消息发送高可用的核心关键所在
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 发送消息延迟容错开关
     */
    private boolean sendLatencyFaultEnable = false;

    /*
      | Producer发送消息消耗时长 | Broker不可用时长 |
      | --------------------— | --------------— |
      | >= 15000 ms           | 600 1000 ms  |
      | >= 3000 ms            | 180 1000 ms  |
      | >= 2000 ms            | 120 1000 ms  |
      | >= 1000 ms            | 60 1000 ms   |
      | >= 550 ms             | 30 * 1000 ms |
      | >= 100 ms             | 0 ms         |
      | >= 50 ms              | 0 ms         |
     */

    /**
     * 延迟级别数组，如 超时 50毫秒，那么服务器是可用的；超时 550 毫秒，那么服务器 30000L 毫秒不可用。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    /**
     * 不可用时长数组
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 容错策略选择消息队列逻辑：
     * <p>
     * 根据 Topic 发布信息选择一个消息队列。主要逻辑如下：
     * 1 容错策略选择消息队列逻辑。优先获取可用队列，其次选择一个broker获取队列，最差返回任意broker的一个队列。
     * 2 未开启容错策略选择消息队列逻辑，直接按 BrokerName 选择一个消息队列，不考虑队列的可用性
     *
     * @param tpInfo         Topic 发布信息
     * @param lastBrokerName brokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 默认情况下容错策略关闭
        // 是否开启失败延时规避机制，该值在消息发送者那里可以设置，如果该值为false,直接从 topic 的所有队列中选择下一个，而不考虑该消息队列是否可用（比如Broker挂掉）。
        if (this.sendLatencyFaultEnable) {
            try {

                /* 优先获取可用队列-选择的队列所属的 Broker 是可用的 */

                // 使用了本地线程变量 ThreadLocal 保存上一次发送的消息队列下标，消息发送使用轮询机制获取下一个发送消息队列。
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                // 对 topic 所有的消息队列进行一次验证，为什么要循环呢？因为加入了发送异常延迟，要确保选中的消息队列(MessageQueue)所在的Broker是正常的
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);

                    // 判断当前的消息队列是否可用（即所在的 Broker 可用），一旦一个 MessageQueue 符合条件，立即返回。
                    // 注意：Topic 所在的所 有Broker全部标记不可用时，进入到下一步逻辑处理。（在此处，我们要知道，标记为不可用，并不代表真的不可用，Broker 是可以在故障期间被运营管理人员进行恢复的，比如重启）
                    // todo 失败延时规避机制就提现在这里，如果消息发送者遇到一次消息发送失败后就会悲观地认为 Broker 不可用，
                    //  在接下来的一段时间内，该 Broker 就不能参与消息发送队列的负载，直到该队列"可用"
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                /* 选择一个broker获取队列，不考虑该队列的可用性 */

                // 根据 Broker 的 startTimestart 进行一个排序，值越小，排前面，然后再选择一个，返回（此时不能保证一定可用，会抛出异常，如果消息发送方式是同步调用，则有重试机制）。
                // 即 选择一个相对好的 Broker，并获得其对应的一个消息队列，不考虑该队列的可用性
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();

                // 根据 Broker 名称获取对应的任一队列的写队列数
                // 获取写队列数量
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // 递增取模算法，选择一个消息队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();

                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }

            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            /*  返回任意broker的一个队列 */
            // 从 Topic 的路由信息中选择一个消息队列，不考虑队列的可用性
            return tpInfo.selectOneMessageQueue();
        }

        // 未开启容错策略选择消息队列逻辑（todo 未开启延迟规避机制，也可以规避故障的 Broker，不过只会在本次消息发送的重试过程中规避该 Broker，下一次消息发送还是会继续尝试）
        // 直接从缓存的队列中选择一个队列，过滤掉发送失败的 Broker。如果队列没有符合的，那么使用自增取模即可
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新延迟容错信息
     * 说明：当 Producer 发送消息时间过长，则逻辑认为N秒内不可用
     *
     * @param brokerName     BrokerName
     * @param currentLatency 延迟时间
     * @param isolation      是否隔离。当开启隔离时，默认延迟为30000。目前主要用于发送消息异常时
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 发送消息延迟容错开关打开
        if (this.sendLatencyFaultEnable) {

            // 计算延迟对应的不可用时长
            // 1 如果隔离，则使用 30s 作为消息发送延迟时间
            // 2 如果不隔离，则使用本次消息发送时延作为消息发送延迟时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);

            // 当 Producer 发送消息时间过长，则逻辑认为N秒内 Broker 不可用
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算延迟对应的不可用时间，计算方法如下：
     * 1 根据消息发送的延迟时间，从 latencyMax 数组尾部向前找到第一个比 currentLatency 小的值的索引 i，如果没有找到返回 0 。
     * 2 找到的话，根据这个索引从 notAvailableDuration 数组中取出对应的时间，这个时间作为 Broker 不可用时间
     *
     * @param currentLatency 消息发送延迟时间
     * @return 不可用时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
