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
package org.apache.rocketmq.broker.topic;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Topic 配置管理，会定时持久化并上报到 NameServer
 * 说明：
 * 1 为了消息发送的高可用，希望新创建的 Topic 在集群中的每台 Broker 上创建对应的队列，避免 Broker 的单节点故障；
 * 2 在 autoCreateTopicEnable 设置为true，表示开启Topic自动创建，但很可能新创建的 Topic 的路由信息只包含在其中一台 Broker 上。
 * - 2.1 默认主题 TBW102 不能发送消息，仅供路由查找。即如果开启自动创建主题，集群中每台 Broker 会将默认主题 TBW102 的路由信息上报到 NameSrv ，客户端可以通过 TBW102 查到对应的路由信息。
 * - 2.2 客户端查到默认主题路由信息后，结合目标 Topic 解析、更新默认主题路由到本地，此时本地的路由信息中的主题是目标 Topic ，路由的其它信息（如读写队列数，权限、所在的 Broker）使用的是默认主题的。此时，Broker 和 NameSrv 还没有目标 Topic 的信息，
 * 在Broker端创建主题的时机为，消息生产者往Broker端发送消息时才会创建，然后Broker端会在一个心跳包周期内，将新创建的路由信息发送到NameServer，于此同时，Broker端还会有一个定时任务，定时将内存中的路由信息，持久化到Broker端的磁盘上。
 * - 2.3 可消息发送方发送消息时，会选择 2.2 中本地的队列中的一个，此时该队列中的 Broker 就是当前发送方要请求的服务，而目标 Topic 也会在该 Broker 上创建，然后 Broker 会上报该目标 Topic 的配置信息到 NameSrv。
 * - 2.4 后续再有客户端访问目标 Topic 的路由信息就能从 NameSrv 中访问到了，而此时目标 Topic 只分布到了一个 Broker 上；当然如果此时访问目标 Topic 时，2.3 中的目标 Topic 还没有上报到 NameSrv ，那么此时会继续执行 2.3 中的逻辑，可能目标 Topic 可以
 * 分布到其它的 Broker 上（取决于选择的队列）。
 *
 * @see AbstractSendMessageProcessor#msgCheck(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, org.apache.rocketmq.remoting.protocol.RemotingCommand)
 */
public class TopicConfigManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final int SCHEDULE_TOPIC_QUEUE_NUM = 18;

    private transient final Lock topicConfigTableLock = new ReentrantLock();

    /**
     * Topic 名到 Topic 配置的映射,
     * todo 特别说明：
     * 1 该集合用于存放 Topic 到 Topic 的配置映射，这里的信息都会上报的到 NameSrv
     * 2 todo 这里数据没有 Broker 的信息，仅仅 Topic 的信息。因为当前类持有 BrokerController 对象，它代表了是哪个 Broker。上报到 NameSrv 是通过 Broker 控制器完成的。
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>(1024);

    private final DataVersion dataVersion = new DataVersion();

    /**
     * 持有的 Broker 控制器
     */
    private transient BrokerController brokerController;

    public TopicConfigManager() {
    }

    /**
     * todo 构造方法，Broker 在初始化时，会设置一些 Topic （系统 Topic）到 TopicConfig 的映射，然后上报到 NameSrv 中。
     * 即：该topicConfigTable中所有的路由信息，会随着Broker向Nameserver发送心跳包中，Nameserver收到这些信息后，更新对应Topic的路由信息表。
     *
     * @param brokerController
     */
    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // 1 SELF_TEST_TOPIC
            String topic = TopicValidator.RMQ_SYS_SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);

            // 读写队列数 1
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 2 todo 如果 Broker 配置了开启 autoCreateTopicEnable ，则默认生成一个 Topic 为 TBW102 的配置项，并放入 topicCofnigTable
            // TBW102 是 Broker 启动时，当 autoCreateTopicEnable 的配置为 true 时，会自动创建该默认 Topic
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);

                // 设置读队列数，默认值为 8
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());

                // 设置写队列数，默认值为 8
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());

                // 可允许继承，可读，可写等权限
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // 3 BenchmarkTest
            String topic = TopicValidator.RMQ_SYS_BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);

            // 读写队列数 1024
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            // 4 DefaultCluster
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();

            // 读写队列数 16
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            // 5 主机名，如 huanglibaodeMacBook-Pro.local
            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            // 读写队列数 1
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 6 OFFSET_MOVED_EVENT
            String topic = TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            // 读写队列数 1
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 7 todo 延时消息主题 SCHEDULE_TOPIC_XXXX
            String topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            // todo 读写队列 18 个，每个队列对应一个延迟级别
            topicConfig.setReadQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            topicConfig.setWriteQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 8 RMQ_SYS_TRACE_TOPIC
            if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
                String topic = this.brokerController.getBrokerConfig().getMsgTraceTopicName();
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                // 读写队列数 1
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // 9 DefaultCluster_REPLY_TOPIC
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            // 读写队列数 1
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 在发送消息的方法中创建 Topic 并上报的 NameSrv。言外之意，以后就可以获取到了该 topic 的路由信息。
     * 说明：
     * 这里也对应了开启自动创建主题的逻辑，当没有手动创建 Topic 时，客户端获取的路由信息时默认主题的，虽然拿到了路由信息，但是任何一个 Broker 上是没有该 Topic 的信息的，
     * 这里正是来创建这个 Topic 的信息的。
     *
     * @param topic                       发送消息时的 Topic
     * @param defaultTopic                发送消息时默认的 Topic 是 TBW102
     * @param remoteAddress
     * @param clientDefaultTopicQueueNums
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
                                                      final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 判断 Broker 中是否存在当前 Topic 对应的缓存，如果有不需要创建。因为这个缓存一定会上报到 NameSrv
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    // 找不到根据defaultTopic继续往下操作
                    // todo  还是要依赖TBW102，来建立 topic 的TopicConfig对象
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        // 如果默认的 topic 为 TBW102
                        if (defaultTopic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                            // 如果不支持自动创建 Topic，则设置权限
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        // todo 如果topic允许继承，此处假设默认topic是 TBW102 ，那么是允许可继承，即此处为true
                        // 基于 TBW102 构建目标 Topic
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            // todo topic 是发送消息的 Topic
                            topicConfig = new TopicConfig(topic);

                            // 取写队列大小，
                            // todo 默认 Topic 的写队列大小 和传入的队列大小（是 DefaultMQProducer.defaultTopicQueueNums 的值 4）进行对比，二者取小，默认主题队列数为 8
                            int queueNums = Math.min(clientDefaultTopicQueueNums, defaultTopicConfig.getWriteQueueNums());
                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            // 设置读写队列数目
                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                    defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                                defaultTopic, remoteAddress);
                    }


                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                                defaultTopic, topicConfig, remoteAddress);

                        // 加入到 Broker 缓存中
                        this.topicConfigTable.put(topic, topicConfig);
                        this.dataVersion.nextVersion();

                        // 是一个新的 Topic
                        createNew = true;

                        // 持久化 TopicConfigManager 中的缓存信息，即持久化到${user.home}\store\config\topics.json
                        this.persist();
                    }
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        // todo 如果是新的 Topic ，那么上报的 NameSrv
        if (createNew) {
            // 通过 Broker 控制器上报 Broker 信息以及 Topic 信息
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    /**
     * 创建 Topic ，todo 即创建重试 Topic 或者死信 Topic
     *
     * @param topic                       重试/死信 Topic
     * @param clientDefaultTopicQueueNums 队列数
     * @param perm
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageBackMethod(
            final String topic,
            final int clientDefaultTopicQueueNums,
            final int perm,
            final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);

        // 存在则返回
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 是否有存在的缓存
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    // 创建新 topic
                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();

                    // 持久化到 Broker 本地文件
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        // 上报到 NameSrv
        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = this.topicConfigTable.get(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    /**
     * 更新 Topic 配置信息，并持久化到文件
     *
     * @param topicConfig
     */
    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        // 更新数据版本
        this.dataVersion.nextVersion();

        // 持久化到文件
        this.persist();
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    /**
     * 构建 Topic 的包装信息
     *
     * @return
     */
    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        // 设置 Topic 相关信息表
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
