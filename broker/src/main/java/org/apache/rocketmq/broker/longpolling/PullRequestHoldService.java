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
import org.apache.rocketmq.store.DefaultMessageStore;

/**
 * PullRequestHoldService方式实现长轮询：
 * <p>
 * 拉取消息请求挂起维护线程服务：
 * 1 当拉取消息请求获得不了消息时，则会将请求进行挂起「给客户端返回 null ，相当于啥都响应」，然后将请求添加到服务的任务队列中。
 * 2 当有符合条件信息时 或 挂起超时时，重新执行获取消息逻辑
 * <p>
 * 长轮询有三个机制实现：
 * 1）PullRequestHoldService 拉取消息请求挂起任务，它会每隔 5s 检查没有拉取到消息的请求是否超时，或对应的请求消息是否到达；
 * 2）DefaultMessageStore#ReputMessageService 消息重放任务，每当有消息到达后就会转发重放消息，然后通知 PullRequestHoldService 有消息到达，PullRequestHoldService 就会遍历挂起的请求对象，
 * 看哪个请求对象对应的消息到达了，然后去拉取消息；
 * 3）如果请求对象超时了，那么不再等待消息到达，直接去尝试拉取消息。
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();

    /**
     * 拉取消息 remote 请求集合
     * key: topic@queueId，标记哪个消费队列
     * value: 拉取消息请求集合
     */
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 添加拉取消息挂起请求到集合( pullRequestTable )，以 Topic + queueId 为维度，拉取消息请求都放到一个集合中
     *
     * @param topic       主题
     * @param queueId     队列编号
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        //  主题 + 队列编号，标志是哪个消费队列
        String key = this.buildKey(topic, queueId);

        // 没有队列的请求集合对象，则新建一个
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        // 将拉取消息请求加入到指定消费队列对应的请求集合中
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
     * todo 通过不断地检查被hold住的请求，检查是否已经有数据了。具体检查那些请求：
     * 是在 ResponseCode.PULL_NOT_FOUND 中调用的 suspendPullRequest 方法添加的
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 根据 长轮训 还是 短轮训 设置不同的等待时间ReputMessageService
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {

                    // 如果开启了长轮询模式，则每隔 5s 去检查是否有需要处理请求。
                    // todo 一般客户端指定的长轮询时间比这个大，默认是 15s
                    this.waitForRunning(5 * 1000);

                } else {
                    /**
                     *  todo 如果不开启长轮询模式，则只挂起一次，挂起时间为 shortPollingTimeMills，然后判断消息是否到达
                     *  todo 因为，获取不到消息时，如果没有开启长轮询，那么设置等待时间就是 1s，这个在下次触发时候即使没有消息，也会达到超时时间。这个叫短轮询。
                     *
                     *             if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                     *                             // 默认为1000ms作为下一次拉取消息的等待时间。
                     *                             pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                     *                         }
                     *
                     * @see PullRequestHoldService#notifyMessageArriving(java.lang.String, int, long, java.lang.Long, long, byte[], java.util.Map)
                     */
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();

                // 检查挂起请求是否有消息到达
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
     * 遍历挂起的拉取消息请求，检查要消息是否到来。
     * <p>
     * 原则：
     * 获取指定messageQueue下最大的offset，然后用来和 拉取任务的待拉取偏移量 来比较，来确定是否有新的消息到来。具体实现在 notifyMessageArriving 方法中。
     * 过程：
     * checkHoldRequest 方法属于主动检查，它是定时检查所有请求，不区分是针对哪个消息队列的拉取消息请求
     */
    private void checkHoldRequest() {
        // 遍历拉取请求集合，遍历指定的 Topic 下某个 queue 是否有消息到达
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                // topic
                String topic = kArray[0];
                // queueId
                int queueId = Integer.parseInt(kArray[1]);

                // todo 根据 Topic 的 queueId 查找队列的最大逻辑偏移量
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {

                    // 根据 queueId 的最大偏移量 offset，判断是否有新的消息到达
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
     * 检查指定队列是否有消息到达，判断依据：
     * todo 比较 待拉取偏移量(pullFromThisOffset) 和 消息队列的最大有效偏移量
     *
     * @param topic        主题
     * @param queueId      队列ID
     * @param maxOffset    消息队列当前最大的逻辑偏移量
     * @param tagsCode     tag hash 码（用于基于 tag 消息过滤），或是延时消息的投递时间
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 过滤位图
     * @param properties   消息属性
     * @see DefaultMessageStore.ReputMessageService#doReput()
     */
    public void notifyMessageArriving(final String topic,
                                      final int queueId,
                                      final long maxOffset,
                                      final Long tagsCode,
                                      long msgStoreTime,
                                      byte[] filterBitMap,
                                      Map<String, String> properties) {

        // 1 标志 topic 下 queue，即哪个消息队列来消息了
        String key = this.buildKey(topic, queueId);

        // 2 获取具体 queue 对应的拉取消息的请求对象
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {

            // 3 获取主题与队列的所有 PullRequest 并清除内部 pullRequest 集合，避免重复拉取
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                // 消息还没有到的请求数组
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                // 4 遍历当前 queue 下每个等待拉取消息的请求
                for (PullRequest request : requestList) {

                    // 4.1 如果待拉取偏移量(pullFromThisOffset)大于消息队列的最大有效偏移量，则再次获取消息队列的最大有效偏移量，再给一次机会。
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 4.2 todo 如果队列最大偏移量大于 pullFromThisOffset 说明有新的消息到达
                    if (newestOffset > request.getPullFromThisOffset()) {

                        // 4.3  对消息根据 tag 属性，使用 MessageFilter 进行一次消息过滤，如果 tag,属性为空，则消息过滤器会返回true
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        // 对消息使用 SQL92 进行过滤，如果消息为空，直接返回 true
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        // todo 4.4 对应消费队列来消息了，但是可能不是感兴趣的。如果是感兴趣的消息，则针对该请求拉取消息，结束长轮询
                        if (match) {
                            try {
                                // 再次尝试去拉取消息，将拉取的结果响应给客户端。
                                // 实际是丢到线程池，获取消息，不会有性能上的问题。
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(
                                        request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }

                            // 一旦处理了就把当前请求移除（不加入到 replayList），继续下一个拉取消息请求
                            continue;
                        }
                    }

                    // todo 4.5 即使没有消息或没有匹配消息，如果超过挂起时间，会再次尝试拉取消息,结束长轮询。
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            // 再次拉取消息,将拉取的结果响应给客户端。
                            // 实际是丢到线程池进行一步的消息拉取，不会有性能上的问题。
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());

                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }

                        // 一旦处理了就把当前请求移除(不加入到 replayList),继续下一个拉取消息请求
                        continue;
                    }


                    // 4.6 不符合唤醒的请求，则将拉取请求重新放入，待下一次检测
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
