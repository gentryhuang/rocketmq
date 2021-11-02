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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息服务实现
 */
public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    /**
     * 事务消息辅助类
     */
    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    /**
     * 消息队列对应的 op 队列
     */
    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * 是否丢弃
     *
     * @param msgExt
     * @param transactionCheckMax
     * @return
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        // 获取消息当前 check 次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            // 如果达到最大检查次数，则跳过
            if (checkTime >= transactionCheckMax) {
                return true;
                // 没有达到，则更新当前消息的 check 次数
            } else {
                checkTime++;
            }
        }

        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 是否需要跳过
     *
     * @param msgExt 消息
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        // 获取消息产生到现在过了多久
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();

        // 如果消息超时，超过 72h，则跳过
        if (valueOfCurrentMinusBorn
                > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
                * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                    msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 重新追加半消息
     *
     * @param msgExt 半消息
     * @param offset 半消息在队列中的逻辑偏移量
     * @return
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {

        // 重新写入半消息
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);

        // 写入半消息成功
        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {

            // 更新消息队列逻辑偏移量
            msgExt.setQueueOffset(
                    putMessageResult.getAppendMessageResult().getLogicsOffset());

            // 更新消息的物理偏移量
            msgExt.setCommitLogOffset(
                    putMessageResult.getAppendMessageResult().getWroteOffset());

            // 更新消息ID
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                    "Send check message, the offset={} restored in queueOffset={} "
                            + "commitLogOffset={} "
                            + "newMsgId={} realMsgId={} topic={}",
                    offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                    msgExt.getTopic());
            return true;
        } else {
            log.error(
                    "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                            + "msgId: {}",
                    msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 事务消息回查
     * todo 总的来说：
     * 1 一条一条拉取，如果在op队列，就是已经commit或者rollback的，不用再管了，否则就检查是否需要回查，需要的话，这条需再写回half队列
     * 2 从 op 队列里确认事务状态，是根据 op 队列里拉取的消息的消息体（保存的是事务半消息的逻辑偏移量）来判断当前偏移的事务消息是否已经处理过了。
     *
     *
     * @param transactionTimeout  检查事务消息的最小时间，只有一条消息超过了可以检查的时间间隔。
     * @param transactionCheckMax 最大回查次数
     * @param listener            When the message is considered to be checked or discarded, the relative method of this class will
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
                      AbstractTransactionalMessageCheckListener listener) {
        try {
            // 事务消息主题，这个是固定的
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;

            // 根据事务消息主题获取 MessageQueue，这个也是固定一个
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }

            log.debug("Check topic={}, queues={}", topic, msgQueues);

            // 遍历 Half MessageQueue ，具体逻辑如下：
            // 1 将 Half 消息与 OP 消息对比，如果 OP 中包含该消息，则不回查
            // 如果不包含，并且 Half 中的该消息存储时间超过了限制时间或最后一条 Op 的存储时间超过了事务超时时间，则进行回查
            for (MessageQueue messageQueue : msgQueues) {
                // 记录开始时间
                long startTime = System.currentTimeMillis();

                // 根据消息队列获取对应的 OP 队列，没有则新创建一个
                MessageQueue opQueue = getOpQueue(messageQueue);

                // 获取 Half 消息队列的当前消费进度（逻辑偏移量）
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);

                // 获取 OP 队列的当前消费进度（逻辑偏移量）
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                            halfOffset, opOffset);
                    continue;
                }

                // op 队列的消息 offset
                List<Long> doneOpOffset = new ArrayList<>();
                // removeMap 表示哪些 half（通过逻辑偏移量 offset 标志） 不需要再回查了（half 消息已经有对应的 op 消息了）
                // key: half 消息逻辑偏移量 offset
                // value: op 消息逻辑偏移量 offset
                HashMap<Long, Long> removeMap = new HashMap<>();

                // 找出需要校验是否需要回查的记录中已经 commit 或 rollback 的消息存放到 removeMap

                /**
                 * 1 从 opQueue 队列中拉取 op 消息，如果 op 消息中记录的消费进度 >= halfOffset ，说明 halfOffset 对应的 half 消息不需要回查了，
                 * 也就是该方法批量拉取 op 消息，然后基于 halfOffset 判断哪些消息进度的 half 消息不需要回查，因为这些 half 消息已经有了 op 消息，
                 * 并把不需要回查的 half 消息的进度以及保存该进度的 op 消息的进度添加到 removeMap 中
                 * 2 基于 halfOffset 判断仍需要回查的当前 op 消息的进度添加到 doneOpOffset 集合中
                 * 3 返回 pullResult 是批量拉取的 op 消息
                 */
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                            messageQueue, halfOffset, opOffset);
                    continue;
                }

                // single thread
                // 空消息的次数
                int getMessageNullCount = 1;

                // todo RMQ_SYS_TRANS_HALF_TOPIC#queueId的最新偏移量，表示 half 消息队列最新消费进度
                long newOffset = halfOffset;

                // RMQ_SYS_TRANS_HALF_TOPIC 的 Half 队列的当前消费进度
                long i = halfOffset;
                // 不断推进 half 消息进度，用于判断是否需要回查当前进度的 half 消息
                while (true) {

                    // 每个 half 消息队列的处理时间是 60s
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }

                    // 如果当前进度（i） 的 half 消息已经commit或者rollback，无需再处理
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);

                        // 从 removeMap 中移除已经确认的 half 消息的进度 ，并将该进度对应的 op 消费进度添加到 doneOpOffset 中
                        Long removedOpOffset = removeMap.remove(i);
                        doneOpOffset.add(removedOpOffset);


                        // 需要回查，即当前进度的 half 消息没有对应的 op 消息
                    } else {

                        // 根据消费进度 i 拉取半消息
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();

                        // 如果消息为空
                        if (msgExt == null) {

                             // 如果超过空消息次数，直接跳出while循环，结束该消息队列的事务状态回查
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            // 没有 half 消息了
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                        messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                        i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        // 1 needDiscard(): 判断当前的 half 消息是否需要丢弃消息，可能已经达到最大的回查此数，默认 15 次
                        // 2 needSkip(): 判断当前的消息是否超过了系统的文件过期时间默认72小时，可broker配置文件中配置
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            // 默认实现是移动到 TRANS_CHECK_MAXTIME_TOPIC 这个topic里
                            listener.resolveDiscardMsg(msgExt);

                            // 继续下条半消息
                            newOffset = i + 1;
                            i++;
                            continue;
                        }


                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                    new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        // 消息已存储的时间
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();

                        // 检测事务状态的时间即开始回查的时间，事务提交后需要一段时间才能开启回查，默认是6秒
                        long checkImmunityTime = transactionTimeout;

                        //获取用户自定义的回查时间
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);

                            // 事务消息的存储时间小于开启回查的间隔时间
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                // 如果要跳过的话，需要把这条消息再追加写入到事务 Topic 的队列中
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            // 新提交的半消息，暂不处理，估计是认为事务也可能没执行完，处理也没意义
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                        checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();

                        // 判断回查是否满足
                        // 正常来说，每条提交/回滚就是已经处理过的消息，在op队列里都有一条消息，如果没有（第一次回查），或者已经有了，但是存放时间已经满足检查条件了，都得回查
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                || (valueOfCurrentMinusBorn <= -1);

                        // 需要回查
                        if (isNeedCheck) {

                            // todo 为了保证消息顺序写入，需要将半消息重新填入 half 队列中
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }

                            // todo 事务回查，确认状态，其中 msgExt 中的 CommitLog 偏移量和 ConsumeQueue 逻辑偏移量都是最新的，上面 putBackHalfMsgQueue 方法执行的结果
                            // 因为回查的方式是 oneway 方式，因此上面的重写入 msgExt 是必要的，在写入消息的时候 TRANSACTION_CHECK_TIMES 回查次数也会写入
                            listener.resolveHalfMsg(msgExt);

                            // 不需要回查
                        } else {

                            // 拉取更多的完成的事务消息，也就是 op 消息，继续筛选
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                    messageQueue, pullResult);
                            continue;
                        }
                    }

                    // 循环下一条 half 消息
                    newOffset = i + 1;
                    i++;
                }

                // 需要更新事务消息消费进度
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                // 计算最新的 op 队列消费进度
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);

                // 如果 op 进度前移，则更新
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * 读取 op 队列，填充 removeMap
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap      Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue        Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset     The current minimum offset of half message queue. 消息队列当前最小偏移量
     * @param doneOpOffset   Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
                                       MessageQueue opQueue,
                                       long pullOffsetOfOp,
                                       long miniOffset, List<Long> doneOpOffset) {

        // 从 OP 队列中读取一定数量的消息，默认最大 32 条
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;

        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            return pullResult;
        }


        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }

        // 遍历拉取到的 OP 队列中的消息，判断一下这些op消息对应的half消息是否处理过了（基于传入的 half 队列的消费进度 miniOffset）
        for (MessageExt opMessageExt : opMsg) {

            // op 队列中存储的内容是 half 队列事务消息已经 commit 和 rollback 的消息的逻辑偏移量 offset
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);

            // 如果该消息是个 删除操作
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                // 消费进度小于消息队列消费进度 ，需要校验
                if (queueOffset < miniOffset) {
                    doneOpOffset.add(opMessageExt.getQueueOffset());

                    // 不需要校验
                    // 存储 half 消息的逻辑偏移量 到 op 消息的逻辑偏移量
                } else {
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap    Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt       Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
                                            MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq     Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums   Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * 从 Op 主题读取 Op 队列中的消息，默认最大 30 条
     * Read op message from Op Topic
     *
     * @param mq     Target Message Queue
     * @param offset Offset in the message queue
     * @param nums   Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * 根据消息队列获取对应的 Op 消息队列
     *
     * @param messageQueue
     * @return
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        // 根据消息队列获取对应的 op 队列
        MessageQueue opQueue = opQueueMap.get(messageQueue);

        // 如果对应的 op 队列为空，则创建一个对应的 op 队列
        if (opQueue == null) {
            // 主题为：RMQ_SYS_TRANS_OP_HALF_TOPIC
            // BrokerName: 同消息队列
            // queueId: 同消息队列
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(),
                    messageQueue.getBrokerName(),
                    messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    /**
     * 拉取半消息
     *
     * @param messageQueue 消息队列
     * @param offset       偏移量
     * @return
     */
    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        // 拉取半消息
        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        // 存储 Op 消息
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
