package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * DemoConsumer
 * <p>
 * desc：
 */
public class DemoPushConsumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {

        // Instantiate with specified consumer group name.
        // 使用指定的用户组名实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer1");

        // Specify name server addresses.
        // NameSrv 地址
        consumer.setNamesrvAddr("localhost:9876");

        // Subscribe one more more topics to consume.
        // 订阅主题
        consumer.subscribe("hlb_topic", "*");

        consumer.subscribe("", "", "");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // Register callback to execute on arrival of messages fetched from brokers.
        // 注册要在从代理获取的消息到达时执行的回调，即注册消息监听器
        // 当从 Broker 拉取到消息时，会执行该监听器的回调方法
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {

                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                System.out.println("具体消息如下：");
                msgs.forEach(messageExt -> {
                    System.out.println(messageExt);

                });

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        //Launch the consumer instance.
        // 启动消费者，初始化一系列组件
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
