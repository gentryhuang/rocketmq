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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * 消息处理队列
 * <p>
 * 1 Queue consumption snapshot 消息队列快照
 * 2 每个 MessageQueue 对应一个 ProcessQueue
 * 3 PullMessageService从消息服务器默认每次拉取32条消息，按照消息的队列偏移量顺序存放在ProcessQueue中，
 * PullMessageService然后将消息提交到消费者消费线程池，消息成功消费后从ProcessQueue中移除。
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
            Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 消息映射读写锁
     */
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
    /**
     * 消息容器
     * <p>
     * 消息映射 - 注意是 TreeMap 结构，根据消费进度排序
     * key: 消息队列进度 value: 消息
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * ProcessQueue 总消息数
     */
    private final AtomicLong msgCount = new AtomicLong();
    /**
     * ProcessQueue 队列最大偏移量
     */
    private final AtomicLong msgSize = new AtomicLong();

    /**
     * 消费锁
     */
    private final Lock consumeLock = new ReentrantLock();

    /**
     * msgTreeMap的一个子集，只在有序消费时使用。
     * todo 用于临时存放从 ProcessQueue 中取出的消息，在消费失败时作为还原数据源，在消费成功时删除即可。
     * <p>
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();

    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long queueOffsetMax = 0L;

    /**
     * 当前 ProcessQueue 是否被丢弃
     */
    private volatile boolean dropped = false;

    /**
     * 上一次拉取消息的时间
     */
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    /**
     * 上一次消费时间戳
     */
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    /**
     * 是否锁定消息处理队列，标识锁定对应的消息队列成功
     */
    private volatile boolean locked = false;
    /**
     * 最后锁定时间
     */
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    /**
     * 是否正在消费
     */
    private volatile boolean consuming = false;

    /**
     * Broker 累计消息数量
     * 计算公式 = queueMaxOffset - 新添加消息数组[n - 1].queueOffset
     * Acc = Accumulation
     * cnt = （猜测）对比度
     */
    private volatile long msgAccCnt = 0;

    /**
     * 消息队列锁是否已过期，默认 30s
     *
     * @return
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理过期消息
     *
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 顺序消息，直接返回
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        // 循环移除消息，每次循环最多移除 16 条
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {

            // 获取第一条消息，判断是否超时，若不超时，则结束循环
            MessageExt msg = null;
            try {
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {

                        break;
                    }
                } finally {
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {

                // 发回超时消息到 Broker
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());

                // 判断此时消息是否依然是第一条，若是，则进行移除
                try {
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 将消息添加到消息处理队列，并返回是否提交给消费者
     *
     * @param msgs
     * @return
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            // 加锁
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;

                // 遍历消息，并写入 msgTreeMap 缓存中
                for (MessageExt msg : msgs) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }

                // 记录消息处理队列中消息的数量
                msgCount.addAndGet(validMsgCnt);

                // 计算是否正在消费
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                // Broker累计消息数量
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 获取消息最大间隔，即消费进度差值
     *
     * @return
     */
    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 移除消息，并返回第一条消息队列位置
     *
     * @param msgs 消息
     * @return 消息队列位置
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    // 这里+1的原因是：如果msgTreeMap为空时，下一条获得的消息位置为queueOffsetMax+1
                    result = this.queueOffsetMax + 1;

                    // 移除消息
                    int removedCnt = 0;
                    // 从 msgTreeMap 中删除该批次的 msg
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }

                    // 递减统计数量
                    msgCount.addAndGet(removedCnt);

                    // 删除后当前msgTreeMap不为空，返回第一个元素，即最小的offset
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * 回滚消费中的消息，即将consumingMsgOrderlyTreeMap中消息重新放在msgTreeMap,并清空consumingMsgOrderlyTreeMap
     * 逻辑类似于
     * {@link #makeMessageToConsumeAgain(List)}
     */
    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 将consumingMsgOrderlyTreeMap消息清除,表示成功处理该批消息，并返回消费进度
     *
     * @return
     */
    public long commit() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                // 获取消费进度，也就是消费队列的逻辑偏移量，类似于数组的下标
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();

                // 维护消息处理队列中的消息条数
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());

                // 维护消息处理队列中的消息大小
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }

                // 移除消息
                this.consumingMsgOrderlyTreeMap.clear();

                if (offset != null) {
                    // 返回消费进度
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 指定消息重新消费
     * 逻辑类似于{@link #rollback()}
     *
     * @param msgs
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    // 将消费失败的消息重新放回 msgTreeMap 中
                    // 注意，放入后会自动排序的，也就是下次消费还是当前放入的消息
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 从消息处理队列顺序获得消息前 batchSixe 条。
     * 注意：
     * 和并发消费获得消息不同，并发消费请求在拉取请求时已经指定了拉取的消息，拉取后直接消费即可
     *
     * @param batchSize 消息条数
     * @return
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            // 获取写锁
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                // 如果消费快照不为空
                if (!this.msgTreeMap.isEmpty()) {

                    // 获取指定大小的消息
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    /**
     * 判断消息处理队列是否存在消息。是根据 读锁判断的
     *
     * @return
     */
    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
