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

import org.apache.rocketmq.store.MessageArrivingListener;

import java.util.Map;

/**
 * 消息到达监听器
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {
    /**
     * 拉取消息请求挂起服务
     */
    private final PullRequestHoldService pullRequestHoldService;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService) {
        this.pullRequestHoldService = pullRequestHoldService;
    }

    /**
     * 消息到达后，通知拉取消息请求挂起服务，消息到达了，可以重试去拉取消息
     *
     * @param topic
     * @param queueId
     * @param logicOffset
     * @param tagsCode
     * @param msgStoreTime
     * @param filterBitMap
     * @param properties
     */
    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                         long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // 通知消息到达
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode,
                msgStoreTime, filterBitMap, properties);
    }
}
