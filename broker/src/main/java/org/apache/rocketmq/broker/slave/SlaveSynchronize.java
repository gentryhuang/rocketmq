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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 从主服务器同步核心元数据信息。
 * <p>
 * 场景分析：
 * 消息消费者首先从主服务器拉取消息，并向其提交消息消费进度，如果当主服务器宕机后，从服务器会接管消息拉取服务，此时消息消费进度存储在从服务器，主从服务器的消息消费进度会出现不一致？
 * 那当主服务器恢复正常后，两者之间的消息消费进度如何同步？
 * 解答：
 * 如果Broker角色为从服务器，会通过定时任务调用syncAll，从主服务器定时以覆盖式的方式同步topic路由信息、消息消费进度、延迟队列处理进度、消费组订阅信息。
 * 进一步问题：
 * 如果主服务器启动后，从服务器马上从主服务器同步消息消息进度，那岂不是又要重新消费？
 * 解答：
 * 其实在绝大部分情况下，就算从服务从主服务器同步了很久之前的消费进度，只要消息者没有重新启动，就不需要重新消费，在这种情况下，RocketMQ提供了两种机制来确保不丢失消息消费进度。
 * 第一种，消息消费者在内存中存在最新的消息消费进度，继续以该进度去服务器拉取消息后，消息处理完后，会定时向Broker服务器反馈消息消费进度，在上面也提到过，在反馈消息消费进度时，会优先选择主服务器，此时主服务器（上线）的消息消费进度就立马更新了，从服务器此时只需定时同步主服务器的消息消费进度即可。
 * 第二种是，消息消费者在向主服务器拉取消息时，如果是是主服务器，在处理消息拉取时，也会更新消息消费进度 @see PullMessageProcessor#processRequest
 * <p>
 * -------------------------------------
 * 主从同步引起的读写分离：
 * 1. 主，从服务器都在运行过程中，消息消费者是从主拉取消息还是从从拉取？
 * 答：默认情况下，RocketMQ消息消费者从主服务器拉取，当主服务器积压的消息超过了物理内存的40%，则建议从从服务器拉取。但如果slaveReadEnable为false，表示从服务器不可读，从服务器也不会接管消息拉取。
 * 2. 当消息消费者向从服务器拉取消息后，会一直从从服务器拉取？
 * 答：不是的。分如下情况：{@link org.apache.rocketmq.store.DefaultMessageStore#getMessage(java.lang.String, java.lang.String, int, long, int, org.apache.rocketmq.store.MessageFilter)}
 * 1）如果从服务器的slaveReadEnable被设置为false，则下次拉取，建议从主服务器拉取。
 * 2）如果从服务器允许读取并且从服务器积压的消息未超过其物理内存的40%，下次拉取使用的Broker为订阅组的brokerId指定的Broker服务器，该值默认为0，代表主服务器。
 * 3）如果从服务器允许读取并且从服务器积压的消息超过了其物理内存的40%，下次拉取使用的Broker为订阅组的whichBrokerWhenConsumeSlowly指定的Broker服务器，该值默认为1，代表从服务器。
 * 读写分离的正确使用方式：
 * 1. 主从 Broker 服务器的 slaveReadEnable 设置为 true
 * 2. 通过updateSubGroup命令更新消息组whichBrokerWhenConsumeSlowly、brokerId，特别是其brokerId不要设置为0，不然从从服务器拉取一次后，下一次拉取就会从主去拉取。
 * {@link PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)}
 */
public class SlaveSynchronize {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    /**
     * 主服务器地址
     */
    private volatile String masterAddr = null;

    /**
     * @param brokerController
     * @see BrokerController#BrokerController(org.apache.rocketmq.common.BrokerConfig, org.apache.rocketmq.remoting.netty.NettyServerConfig, org.apache.rocketmq.remoting.netty.NettyClientConfig, org.apache.rocketmq.store.config.MessageStoreConfig)
     */
    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    /**
     * 从主服务器同步
     *
     * @see BrokerController#start()
     */
    public void syncAll() {
        // 1 同步 Topic 配置 - 覆盖式
        this.syncTopicConfig();

        // 2 同步消息消费进度 - 覆盖式
        this.syncConsumerOffset();

        // 3 同步延迟队列调度进度 - 覆盖式
        this.syncDelayOffset();

        // 4 同步消费组订阅信息 - 覆盖式
        this.syncSubscriptionGroupConfig();
    }

    /**
     * 从主服务器同步 Topic 配置
     */
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                TopicConfigSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);

                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                        .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                            .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();

                    // 保存 Topic 配置
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                            .putAll(topicWrapper.getTopicConfigTable());
                    // 持久化 Topic 配置
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务器同步消息消费进度
     */
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);

                // 保存消费组-主题下每个队列的消费进度
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                        .putAll(offsetWrapper.getOffsetTable());
                // 持久化
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务同步延迟队列调度进度
     */
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务器同步消费组订阅信息
     */
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                            this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                            subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();

                    // 保存消费组订阅信息
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                            subscriptionWrapper.getSubscriptionGroupTable());

                    // 持久化
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
