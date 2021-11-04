package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * DemoProducer
 *
 * @author <a href="mailto:libao.huang@yunhutech.com">shunhua</a>
 * @since 2021/10/08
 * <p>
 * desc：
 * 1 删除 store 下的存储
 * 2 删除 logs 下的 RocketMQ 日志
 */
public class DemoProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("demo_producer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 1; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );

            msg.setDelayTimeLevel(3);

            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg, 50000);
            System.out.printf("%s%n", sendResult);
            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                //...
            }
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
