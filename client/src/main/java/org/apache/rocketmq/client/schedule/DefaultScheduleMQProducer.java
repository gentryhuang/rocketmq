package org.apache.rocketmq.client.schedule;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.schedule.ScheduleMessageConst;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DefaultScheduleMQProducer
 * <p>
 * desc：
 */
public class DefaultScheduleMQProducer extends DefaultMQProducer implements ScheduleMQProducer {

    public DefaultScheduleMQProducer(final String producerGroup) {
        super(producerGroup);
    }

    @Override
    public SendResult send(Message msg, long timeout, long delayTimeMillis) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        // 设置延时时间
        Map<String, String> properties = msg.getProperties();
        properties.put(ScheduleMessageConst.PROPERTY_DELAY_TIME, String.valueOf(delayTimeMillis));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    @Override
    public SendResult send(Message msg, long timeout, int delayTime, TimeUnit timeUnit) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }
}
