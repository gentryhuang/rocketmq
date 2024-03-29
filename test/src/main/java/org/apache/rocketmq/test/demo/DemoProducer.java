package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

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

        //Create a message instance, specifying topic, tag and message body.

        for (int i = 1; i < 10000000; i++) {
            Message msg = new Message("hlb_topic" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //  msg.setDelayTimeLevel(3);

            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg, 50000);
            System.out.printf("%s%n", sendResult);
        }


        //Shut down once the producer instance is not longer in use.
        //  producer.shutdown();


    }
}
