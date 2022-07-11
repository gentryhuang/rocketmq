package org.apache.rocketmq.test.demo;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.schedule.DefaultScheduleMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Random;
import java.util.concurrent.TimeUnit;

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


            for (int i = 1; i < 12; i++) {
                Message msg = new Message("hlb_topic" /* Topic */,
                        "TagB" /* Tag */,
                        ("Hello RocketMQ " +
                                i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                //  msg.setDelayTimeLevel(3);

                //Call send message to deliver message to one of brokers.

                long delayTimeMills = random.nextInt(1000 * 60 * 60 * 12);

                // SendResult sendResult = producer.send(msg, 50000, System.currentTimeMillis() + delayTimeMills);

               // SendResult sendResult = producer.send(msg, 50000, 1657495870000L);
               SendResult sendResult = producer.send(msg, 5000, 120, TimeUnit.MINUTES);
                System.out.printf("%s%n", sendResult);
            }



        //Shut down once the producer instance is not longer in use.
        //  producer.shutdown();
        /*
          投递消息如下：

          2022-07-10 23:10:10  1657465810000
          2022-07-10 23:30:10  1657467010000
          2022-07-10 23:50:10  1657468210000
          2022-07-11 00:00:10  1657468810000
          2022-07-11 03:00:10  1657479610000
          2022-07-11 06:00:10  1657490410000
          2022-07-11 08:00:10  1657497610000
          2022-07-11 07:00:10  1657494010000
          2022-07-11 07:31:10  1657495870000
         */


        /*
           延时消息待优化：
           1 增加个消息类型，使用 systemFlag 区分更优雅；
           2 消息进一步的可靠性保证，如果投递到 CommitLog 失败怎么处理（pageCache 繁忙等）；
             - 重试，允许通过扫描线程任务重新加入时间轮
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
