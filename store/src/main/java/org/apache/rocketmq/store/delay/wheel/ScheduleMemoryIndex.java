package org.apache.rocketmq.store.delay.wheel;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.schedule.ScheduleMessageConst;
import org.apache.rocketmq.common.schedule.tool.ScheduleConfigHelper;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.delay.ScheduleLog;
import org.apache.rocketmq.store.delay.ScheduleMessageStore;

import java.util.concurrent.ConcurrentMap;

/**
 * MemoryIndex
 */
public class ScheduleMemoryIndex implements TimerTask {
    /**
     * 延时时间（触发时间）
     */
    private Long triggerTime;

    /**
     * 消息在 ScheduleLog 中的物理偏移量
     */
    private Long offset;

    /**
     * 消息大小
     */
    private Integer size;

    private ScheduleMessageStore scheduleMessageStore;


    public ScheduleMemoryIndex(ScheduleMessageStore scheduleMessageStore, Long triggerTime, Long offset, Integer size) {
        this.scheduleMessageStore = scheduleMessageStore;
        this.triggerTime = triggerTime;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        // 查找属于哪个 ScheduleLog
        Long delayPartitionDirectory = ScheduleConfigHelper.getDelayPartitionDirectory(triggerTime);
        ScheduleLog scheduleLog = scheduleMessageStore.getScheduleLogManager().getScheduleLogTable().get(delayPartitionDirectory);
        if (scheduleLog == null) {
            return;
        }

        // 查找消息
        // 2.5 根据偏移量和消息大小从 Commitlog 中获取对应的延时消息
        MessageExt msgExt = scheduleMessageStore.lookMessageByOffset(scheduleLog, offset, size);

        if (msgExt != null) {
            try {

                // 判断消息是否投递过，注意这里不能加 =
                // todo 但是可能会有重复投递的情况，比如在初始化加载消息文件时，会把文件消息加载到时间轮，但是 == triggerTime 的消息不能过滤，尽管这里消息之前已经投递过
                if (scheduleMessageStore.getScheduleLogManager().getScheduleDelayTimeTable().getOrDefault(delayPartitionDirectory, 0L) > triggerTime) {
                    System.out.println(ScheduleConfigHelper.getCurrentDateTime() + " 时间轮触发，但 msgExt 已经被投递过 " + msgExt.getMsgId());
                    return;
                }

                // 2.6 还原延时消息真实属性
                MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);

                // 投递真正的消息，这时候没有了延时消息的标志了
                PutMessageResult putMessageResult = scheduleMessageStore.getMessageStore().putMessage(msgInner);

                // 如果发送成功，则继续下一个消息索引的获取与判断是否到期
                if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    System.out.println(ScheduleConfigHelper.getCurrentDateTime() + " 时间轮调度延时任务 - 消息投递，msg: " + msgExt.getMsgId());
                    System.out.println();

                    // todo 记录投递成功的物理偏移量，需要持久化
                    scheduleMessageStore.getScheduleLogManager().getScheduleDelayTimeTable().put(delayPartitionDirectory, triggerTime);

                    // 消息投递失败
                } else {
                    System.out.println("时间轮触发，但 msgExt投递失败 " + msgExt);
                    // FIXME 重试
                    return;
                }
            } catch (Exception e) {
                /*
                 * XXX: warn and notify me
                 */
                System.out.println("时间轮触发，但 msgExt投递失败，异常信息：" + e);

            }
        }
    }

    /**
     * 组装要投递的消息
     *
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        // 清理掉延时标志，不需要再次延时了
        MessageAccessor.clearProperty(msgInner, ScheduleMessageConst.PROPERTY_DELAY_TIME);
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());
        return msgInner;
    }

    public Long getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(Long triggerTime) {
        this.triggerTime = triggerTime;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public ScheduleMessageStore getScheduleMessageStore() {
        return scheduleMessageStore;
    }

    public void setScheduleMessageStore(ScheduleMessageStore scheduleMessageStore) {
        this.scheduleMessageStore = scheduleMessageStore;
    }

}
