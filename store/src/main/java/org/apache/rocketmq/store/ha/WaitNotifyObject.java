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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 等待-通知对象
 */
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 线程ID 到唤醒标志的映射
     */
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable = new HashMap<Long, Boolean>(16);

    /**
     * 标志是否通知了
     */
    protected volatile boolean hasNotified = false;

    /**
     * 通知-唤醒
     */
    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    /**
     * 超时等待
     *
     * @param interval
     */
    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    /**
     * 唤醒所有
     */
    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;

            for (Map.Entry<Long, Boolean> entry : this.waitingThreadTable.entrySet()) {
                // 如果存在等待唤醒的线程，那么就设置唤醒标记
                needNotify = needNotify || !entry.getValue();

                // 设置标记为 true
                entry.setValue(true);
            }

            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    /**
     * 超时等待
     *
     * @param interval
     */
    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            // 获取当前线程等待状态，让其等待
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {

                // 设置标记为 false
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
