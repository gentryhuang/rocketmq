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
        if (!isCanSchedule(delayTimeMillis)) {
            throw new MQClientException("the delay time is too close to the current", null);
        }
        msg.setTopic(withNamespace(msg.getTopic()));
        // 设置延时时间
        Map<String, String> properties = msg.getProperties();
        properties.put(ScheduleMessageConst.PROPERTY_DELAY_TIME, String.valueOf(delayTimeMillis));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    @Override
    public SendResult send(Message msg, long timeout, long delayTime, TimeUnit timeUnit) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long delayTimeMills = getTriggerTimeMillis(delayTime, timeUnit);
        if (!isCanSchedule(delayTimeMills)) {
            throw new MQClientException("the delay time is too close to the current", null);
        }
        msg.setTopic(withNamespace(msg.getTopic()));
        // 设置延时时间
        Map<String, String> properties = msg.getProperties();
        properties.put(ScheduleMessageConst.PROPERTY_DELAY_TIME, String.valueOf(delayTimeMills));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * 是否调度延时消息
     *
     * @param delayTimeMillis 延时时间
     * @return
     */
    private boolean isCanSchedule(long delayTimeMillis) {
        return ScheduleMessageConst.MIN_DELAY_GRANULARITY > (System.currentTimeMillis() - delayTimeMillis);
    }

    /**
     * 计算任务触发时间戳
     *
     * @param delayTime
     * @param timeUnit
     * @return
     */
    private Long getTriggerTimeMillis(Long delayTime, TimeUnit timeUnit) {
        if (delayTime == null || delayTime < 0) {
            return 0L;
        }
        return timeUnit.toMillis(delayTime) + System.currentTimeMillis();
    }

}
