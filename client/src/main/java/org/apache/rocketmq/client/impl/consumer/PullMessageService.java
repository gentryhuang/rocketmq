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

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

/**
 * 负责对消息队列进行消息拉取，从远端服务器（Broker 使用存储服务）拉取消息后将消息存储 ProcessQueue 消息队列处理中，
 * 并提交消费任务到 ConsumeMessageService，使用线程池来消费消息，确保了消息拉取与消息消费的解耦。
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 拉取消息请求队列
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    /**
     * 客户端实例，消息消费者对应的客户端实例
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
     * 将拉取消息请求放入队列中。有后台线程阻塞等待任务。
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
     * todo 特别说明：
     * 1 RocketMQ 未真正实现消息推模式，而是消费者主动向消费服务器拉取消息。RocketMQ推模式是循环向消息服务端发起消息拉取请求。
     *
     * @param pullRequest 拉取消息请求
     */
    private void pullMessage(final PullRequest pullRequest) {
        // 1 根据 消费组（名）从 JVM 实例中获取消费者实例，也就是当前消费者客户端
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            //强转为推送模式消费者
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;

            // 对比用的
            //  DefaultMQPullConsumer impl1 = (DefaultMQPullConsumer)consumer;
            //  impl1.pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums)

            // 当前消费端实例 从 Broker 拉取消息，拉取到消息后提交给消费线程池。
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        // 不断获取 拉取消息请求，来一个请求就立马进行消息拉取，没有就阻塞等待。
        // 想想看，pullRequestQueue 队列中的 PullRequest 哪来的？
        // - PullRequest 随着消费者分配到新的队列而创建，然后添加而来
        // - 在拉取消息的逻辑中（pullMessage方法），指定下次拉取消息请求，然后添加而来
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
