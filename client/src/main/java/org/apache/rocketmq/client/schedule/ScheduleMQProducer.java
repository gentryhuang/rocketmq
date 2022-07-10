package org.apache.rocketmq.client.schedule;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * ScheduleMQProducer
 * desc：
 */
public interface ScheduleMQProducer extends MQProducer {

    /**
     * 发送指定延时时间的消息
     *
     * @param msg
     * @param timeout
     * @param delayTimeMillis
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final long timeout, final long delayTimeMillis) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 发送指定时间粒度的延时消息
     *
     * @param msg
     * @param timeout
     * @param delayTime
     * @param timeUnit
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final long timeout, long delayTime, TimeUnit timeUnit) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;
}
