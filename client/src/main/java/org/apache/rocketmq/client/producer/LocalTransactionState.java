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
package org.apache.rocketmq.client.producer;

/**
 * 本地事务状态
 */
public enum LocalTransactionState {
    /**
     * 提交
     * 提交事务，它允许消费者消费
     */
    COMMIT_MESSAGE,
    /**
     * 回滚
     * 回滚事务，它代表该消息将被删除，不允许被消费。
     */
    ROLLBACK_MESSAGE,
    /**
     * 未知
     * 中间状态，它代表需要检查消息队列来确定状态
     */
    UNKNOW,
}
