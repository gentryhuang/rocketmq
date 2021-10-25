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

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

/**
 * 拉取消费服务，不断从 Broker 拉取消息并提交消费任务到 ConsumeMessageService，进行消费消息逻辑
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 拉取消息请求队列
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    /**
     * MQClient 对象
     */
    private final MQClientInstance mQClientFactory;
    /**
     * 定时器，用于提交延迟拉取请求
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 执行延迟拉取消息请求
     *
     * @param pullRequest 拉取消息请求
     * @param timeDelay   延迟时长
     */
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 将拉取消息请求放入队列中，然后等待待执行
     *
     * @param pullRequest 拉取消息请求
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    /**
     * 执行延迟任务
     *
     * @param r
     * @param timeDelay
     */
    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * 拉取消息
     *
     * @param pullRequest 拉取消息请求
     */
    private void pullMessage(final PullRequest pullRequest) {
        // 根据 消费组（名） 获取消费者
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;

            // 从 Broker 拉取消息，并消费
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        // 循环拉取消息请求队列 pullRequestQueue ，进行消息拉取
        while (!this.isStopped()) {
            try {
                // 从拉取请求队列中不断获取拉取消息的请求
                // 当提交了消息拉取请求后，立马执行拉取消息，没有的话就阻塞等待
                PullRequest pullRequest = this.pullRequestQueue.take();

                // 拉取消息
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
