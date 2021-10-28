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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**
 * PullRequestHoldService方式实现长轮询：
 * <p>
 * 拉取消息请求挂起维护线程服务：
 * 1 当拉取消息请求获得不了消息时，则会将请求进行挂起，添加到该服务
 * 2 当有符合条件信息时 或 挂起超时时，重新执行获取消息逻辑
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();

    /**
     * 拉取消息 remote 请求集合
     * key: topic@queueId
     */
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 添加拉取消息挂起请求到集合( pullRequestTable )
     *
     * @param topic       主题
     * @param queueId     队列编号
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    /**
     * 根据 主题 + 队列编号 创建唯一标识
     *
     * @param topic
     * @param queueId
     * @return
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * 定时检查挂起请求是否有需要通知重新拉取消息并进行通知
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 根据 长轮训 还是 短轮训 设置不同的等待时间
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 如果开启长轮询，则每隔 5 秒判断消息是否到达
                    this.waitForRunning(5 * 1000);
                } else {
                    // 没有开启长轮询，每隔 1s 再次尝试
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }


                long beginLockTimestamp = this.systemClock.now();

                // 检查挂起请求是否有需要通知的
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 遍历挂起的拉取消息请求，检查要消息是否到来
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);

                // 判断当前 Topic 下的 queueId 对应的队列是否有消息到达
                // 即获取 Topic 的 queueId 最大 offset
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    // 通知有消息到达
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * 检查是否有需要通知的请求
     *
     * @param topic     主题
     * @param queueId   队列编号
     * @param maxOffset 消费队列最大 offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 检查指定队列是否有需要通知的请求
     *
     * @param topic        主题
     * @param queueId      队列编号
     * @param maxOffset    消息队列最大 ofset
     * @param tagsCode     tag hash 码
     * @param msgStoreTime
     * @param filterBitMap
     * @param properties
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
                                      long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        // 标志 topic 下 queue
        String key = this.buildKey(topic, queueId);
        // 获取具体 queue 对应的拉取消息的请求对象
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                // 消息还没有到的请求数组
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                // 遍历每个等待拉取消息的 请求
                for (PullRequest request : requestList) {
                    // 如果 maxOffset 过小，则重新读取一次。
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 1 如果拉取消息偏移大于请求偏移量，说明有新的匹配消息，唤醒请求，即再次拉取消息。
                    if (newestOffset > request.getPullFromThisOffset()) {

                        // 验证消息是否是自己感兴趣的消息，过滤
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        // 如果是则拉取消息
                        if (match) {
                            try {
                                // 再次尝试去拉取消息。
                                // 实际是丢到线程池，获取消息，不会有性能上的问题。
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }

                            // 继续下一个拉取消息请求
                            continue;
                        }
                    }

                    // 2 即使没有消息或没有匹配消息，如果超过挂起时间，会再次拉取消息。
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            // 唤醒请求，再次拉取消息。实际是丢到线程池进行一步的消息拉取，不会有性能上的问题。
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    // 不符合唤醒的请求重新添加到集合(pullRequestTable)
                    replayList.add(request);
                }

                // 添加回去
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
