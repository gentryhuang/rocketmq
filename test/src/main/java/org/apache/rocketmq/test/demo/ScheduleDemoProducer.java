package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.schedule.DefaultScheduleMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Random;

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
public class ScheduleDemoProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultScheduleMQProducer producer = new
                DefaultScheduleMQProducer("demo_producer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();



        Random random = new Random(1000 * 60 * 60 * 12);

        //Create a message instance, specifying topic, tag and message body.

        for (int i = 1; i < 1000000; i++) {
            Message msg = new Message("hlb_topic" /* Topic */,
                    "TagB" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //  msg.setDelayTimeLevel(3);

            //Call send message to deliver message to one of brokers.

            long delayTimeMills = random.nextInt(1000 *60 *60*12);


            SendResult sendResult = producer.send(msg, 50000, System.currentTimeMillis() + delayTimeMills);
            System.out.printf("%s%n", sendResult);
        }


        //Shut down once the producer instance is not longer in use.
        //  producer.shutdown();
        /*
          投递消息如下：
          1.  2022-07-10 07:00:30   - 1657407630000L
          2.  2022-07-10 07:29:30   - 1657409370000L
          3.  2022-07-10 07:30:30   - 1657409430000L
          4.  2022-07-10 07:31:00   - 1657409460000L
          5.  2022-07-10 08:01:00   - 1657411260000L
          6.  2022-07-10 08:31:00   - 1657413060000L
          7.  2022-07-09 22:55:00   - 1657378500000L
          8.  2022-07-10 00:00:00   - 1657382400000L
          9.  2022-07-09 23:17:10   - 1657379830000L
         */


        /*
           延时消息待优化：
           1 增加个消息类型，使用 systemFlag 区分更优雅；
           2 消息进一步的可靠性保证，如果投递到 CommitLog 失败怎么处理（pageCache 繁忙等）；
             - 重试
           3 消息删除问题，要考虑到强制删除的情况，磁盘空间和消息的有效性要考虑到
           4 延时消息不能在从节点进行工作，因为一旦工作就会把消息写入到从节点的 CommitLog 中，而主从同步架构，从节点的消息要依据主节点；
           5 顺序消息的情况能否保证
           6 主从同步，需要同步延时消息吗？
             - 需要，但是只做备份
           7 精确度，是否考虑补偿机制，如过期5分钟内进行补偿
           8 消息卡点丢失问题 - 拉取消息补偿几分钟，基于当前时间 - 5 分钟
           9 同步异步刷盘的支持问题
         */


    }

}
