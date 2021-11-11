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

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * RocketMQ 使用了一个设计模式：门面模式(Facade Pattern)
 * 门面模式主要的作用是给客户端提供了一个可以访问系统的接口，隐藏系统内部的复杂性。
 */
public interface MQProducer extends MQAdmin {

    /*
     * 在 Producer 的接口 MQProducer 中，定义了多个不同参数的发消息的方法，按照发送方式不同可以分成三类：
     * 单向发送（Oneway）：发送消息后立即返回，不处理响应，不关心是否发送成功；
     * 同步发送（Sync）：发送消息后等待响应；
     * 异步发送（Async）：发送消息后立即返回，在提供的回调方法中处理响应。
     */

    void start() throws MQClientException;

    void shutdown();

    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    SendResult send(final Message msg, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;

    void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    void sendOneway(final Message msg) throws MQClientException, RemotingException,
            InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException;

    /**
     * Producer 顺序发送
     *
     * @param msg
     * @param selector
     * @param arg
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException;

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final Object arg) throws MQClientException;

    //for batch
    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void send(final Collection<Message> msgs, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    //for rpc
    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final RequestCallback requestCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                 final RequestCallback requestCallback,
                 final long timeout) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
