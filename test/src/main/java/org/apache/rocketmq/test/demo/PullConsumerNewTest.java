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

package org.apache.rocketmq.test.demo;


import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


@SuppressWarnings("deprecation")

public class PullConsumerNewTest {


    public static void main(String[] args) throws MQClientException {

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull_group_name1");


        // Specify name server addresses.
        // namesrv 地址
        consumer.setNamesrvAddr("localhost:9876");
        Set<String> topics = new HashSet<>();

        //You would better to register topics,It will use in rebalance when starting

        topics.add("hlb_topic");

        // 注册 Topic ，用于订阅
        consumer.setRegisterTopics(topics);
        consumer.start();
        ExecutorService executors = Executors.newFixedThreadPool(topics.size(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullConsumerThread");
            }
        });

        for (String topic : consumer.getRegisterTopics()) {
            executors.execute(new Runnable() {

                public void doSomething(List<MessageExt> msgs) {
                    System.out.println(Thread.currentThread().getName() + " 正在消费消息 " + msgs);
                }


                @Override
                public void run() {
                    while (true) {
                        try {
                            // 获取当前消费者分配的队列
                            Set<MessageQueue> messageQueues = consumer.fetchMessageQueuesInBalance(topic);
                            if (messageQueues == null || messageQueues.isEmpty()) {
                                Thread.sleep(1000);
                                continue;
                            }

                            System.out.println("messageQueues: " + messageQueues);

                            // 遍历队列
                            PullResult pullResult = null;
                            for (MessageQueue messageQueue : messageQueues) {
                                try {
                                    // 基于当前消费者所属的组，获取 messageQueue 的偏移量
                                    long offset = this.consumeFromOffset(messageQueue);
                                    System.out.println(" messageQueue-" + messageQueue.getQueueId() + " offset:" + offset);

                                    pullResult = consumer.pull(messageQueue, "*", offset, 32);
                                    System.out.println("pullResult: " + pullResult);

                                    switch (pullResult.getPullStatus()) {
                                        case FOUND:
                                            List<MessageExt> msgs = pullResult.getMsgFoundList();
                                            if (msgs != null && !msgs.isEmpty()) {
                                                this.doSomething(msgs);

                                                // todo 手动更新消费进度
                                                //update offset to broker
                                                consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                                //print pull tps
                                                this.incPullTPS(topic, pullResult.getMsgFoundList().size());

                                                // todo 验证消费进度，即返回的下一个进度 和 获取的消费进度 比较
                                                // todo 两者一致
                                                long currentOffset = this.consumeFromOffset(messageQueue);
                                                System.out.println(currentOffset == pullResult.getNextBeginOffset());

                                            }
                                            break;
                                        case OFFSET_ILLEGAL:
                                            consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                            break;
                                        case NO_NEW_MSG:
                                            Thread.sleep(1);
                                            consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                            break;

                                        case NO_MATCHED_MSG:
                                            consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                            break;
                                        default:

                                    }

                                } catch (RemotingException e) {
                                    e.printStackTrace();
                                } catch (MQBrokerException e) {
                                    e.printStackTrace();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                        } catch (MQClientException e) {
                            //reblance error
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            Thread.sleep(3000);
                        } catch (Exception ex) {

                        }
                    }
                }

                public long consumeFromOffset(MessageQueue messageQueue) throws MQClientException {
                    //-1 when started
                    long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
                    if (offset < 0) {
                        //query from broker
                        offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
                    }

                    if (offset < 0) {
                        //first time start from last offset
                        offset = consumer.maxOffset(messageQueue);

                    }

                    //make sure
                    if (offset < 0) {
                        offset = 0;
                    }
                    return offset;
                }


                public void incPullTPS(String topic, int pullSize) {
                    consumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory()
                            .getConsumerStatsManager().incPullTPS(consumer.getConsumerGroup(), topic, pullSize);

                }
            });

        }

//        executors.shutdown();

//        consumer.shutdown();

    }

}
