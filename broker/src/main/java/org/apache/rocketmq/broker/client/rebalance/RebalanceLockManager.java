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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageQueue;

public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);

    /**
     * 锁最大存活时间，默认 60s
     */
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));

    private final Lock lock = new ReentrantLock();

    /**
     * 消费组下 MessageQueue 的锁定映射表，按消费组分组。
     * todo
     * 1 不同消费组的消费者可以同时锁定同一个消息消费队列
     * 2 集群模式下同一个消费组内只能被一个消费者锁定
     * todo 本质上是同一消费组中的消费者锁定 MessageQueue ，
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable = new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    /**
     * 尝试锁住 mq
     *
     * @param group
     * @param mq
     * @param clientId
     * @return
     */
    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        // 判断有没有已经锁住，锁住的话刷新过期时间 60s
        // 没有锁住的情况：1）当前 group-mq 下的锁对象中的客户端不是传入的 clientId   2）是传入的 clientId 但是锁过期了
        if (!this.isLocked(group, mq, clientId)) {
            try {
                // 获取 JVM 实例锁
                // todo 因为每个 Broker 维护自己的队列锁，并不共享
                this.lock.lockInterruptibly();
                try {
                    // 尝试获取，判断是否存在，存在就判断是否过期
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        // 创建锁对象
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                    }

                    // 占据锁对象的是传入的 clientId ，刷新过期时间并结束
                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    // 占领锁对象的不是传入的 clientId ，那么需要判断这个锁对象是否过期
                    String oldClientId = lockEntry.getClientId();
                    // 过期的话，就可以重置了；即传入的 clientId 就可以占据了
                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());

                        // 告警日志
                        log.warn(
                                "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                        return true;
                    }

                    log.warn(
                            "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);

                    // group 下的别的 clientId 占据了队列的锁对象，而且并没有过期，那么就无法获取锁
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 判断消费组 group 下的消息队列 mq 是否被 clientID 锁定，可能情况如下：
     * <p>
     * 1）当前 group 下还没有客户端实例锁定这个 mq，那么传入的 clientId 直接锁定即可；
     * 2）当前 group 下正好是传入的 clientId 锁定的，而且还没过期，那么刷新过期时间即可；
     * 3）当前 group 下正好是传入的 clientId 锁定的，但是过期了，那么重置过期时间即可；
     * 4）当前 group 下锁住 mq 的不是传入的 clientId 而是其它客户端实例：
     * - 如果其它客户端实例持有该 mq 的 LockEntry 并没有过期，那么传入的 clientId 就获取锁失败；
     * - 如果其它客户端实例持有该 mq 的 LockEntry 过期，那么传入的 clientId 可以占据 mq 的 LockEntry
     *
     * @param group    消费组
     * @param mq       消息队列
     * @param clientId 消费组下某个消费端ID
     * @return
     */
    private boolean isLocked(final String group,
                             final MessageQueue mq,
                             final String clientId) {
        // 取出当前 Broker 上指定消费组 group 下的消息队列锁定表
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            // 获取 mq 对应的锁对象
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                // 判断锁对象锁定的是否是目前消费端实例 & 锁还有效
                boolean locked = lockEntry.isLocked(clientId);

                // 锁有效，刷新过期时间
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * clientID 尝试批量锁定 MessageQueue
     *
     * @param group    消费组
     * @param mqs      队列集合
     * @param clientId 消费者ID
     * @return
     */
    public Set<MessageQueue> tryLockBatch(final String group,
                                          final Set<MessageQueue> mqs,
                                          final String clientId) {

        // 锁定成功的 MessageQueue 集合结果集
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 没有锁住的 MessageQueue 结合结果集
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 遍历要锁定的 MessageQueue
        for (MessageQueue mq : mqs) {
            // 判断当前客户端是否已经锁定了当前队列
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);

            } else {
                notLockedMqs.add(mq);
            }
        }

        // 存在没有被锁定的 MessageQueue，尝试锁定这些队列
        if (!notLockedMqs.isEmpty()) {
            try {
                // jvm 锁
                this.lock.lockInterruptibly();
                try {
                    // 消费组不在所锁定map中，加入锁定的map中
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 对没有锁定的 MessageQueue 尝试当前消费端实例锁定
                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            // 创建锁对象
                            lockEntry = new LockEntry();
                            // 维护哪个消费端实例占有 mq
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                    "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                    group,
                                    clientId,
                                    mq);
                        }

                        // 如果是当前客户端占有 mq，则更新时间即可
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();
                        // 锁过期，那么当前客户端占有该锁
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                    "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                    group,
                                    oldClientId,
                                    clientId,
                                    mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                                "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        // 返回锁定的 MessageQueue
        return lockedMqs;
    }

    /**
     * clientID 尝试批量释放自己持有的 MessageQueue 的锁定，不是自己持有的不能释放
     *
     * @param group    消费组
     * @param mqs      消费队列
     * @param clientId 消费组下的消费者实例
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                // 获取当前 Broker 上消费组下消费队列的锁定情况
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    // 遍历要释放锁定的 MessageQueue
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);

                        // 存在该 mq 的锁定对象
                        if (null != lockEntry) {

                            // 判断是否是传入消费者所持有的，如果是则移除锁定信息即可
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                        group,
                                        mq,
                                        clientId);

                                // 不是传入消费者所持有的，不能移除该锁定
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                        lockEntry.getClientId(),
                                        group,
                                        mq,
                                        clientId);
                            }

                            // 不存在该 mq 的锁定对象
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                        }
                    }

                    // 该消费组下没有锁定信息，忽略释放锁定
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                            group,
                            clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    /**
     * MessageQueue 对应锁对象
     */
    static class LockEntry {
        /**
         * 消费端实例id
         */
        private String clientId;
        /**
         * 锁对象更新时间
         */
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        /**
         * 重置更新时间
         *
         * @param lastUpdateTimestamp
         */
        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 是否锁定
         *
         * @param clientId
         * @return
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        /**
         * 是否过期。根据当前时间和上次更新时间差，和过期阈值（60s) 比对
         *
         * @return
         */
        public boolean isExpired() {
            boolean expired =
                    (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
