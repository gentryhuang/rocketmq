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
package org.apache.rocketmq.store.config;

import java.io.File;

/**
 * 存储路径配置辅助类，主要用于配置相关持久化文件的目录解析
 */
public class StorePathConfigHelper {

    /**
     * 获取消息队列文件夹路径，默认：$user.home/store/consumequeue
     * @param rootDir
     * @return
     */
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    /**
     * 获取消息队列文件夹扩展路径，默认：$user.home/store/consumequeue_ext
     * @param rootDir
     * @return
     */
    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return rootDir + File.separator + "consumequeue_ext";
    }

    /**
     * 获取消息索引文件夹路径，默认：$user.home/store/index
     * @param rootDir
     * @return
     */
    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    /**
     * 获取 CommitLog、ConsumeQueue、Index 文件刷盘点文件，默认：$user.home/store/checkpoint
     * @param rootDir
     * @return
     */
    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    /**
     * 获取标志 Broker 是否异常退出文件，默认：$user.home/store/abort
     * @param rootDir
     * @return
     */
    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }

    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }

    /**
     * 获取延迟消息偏移量文件，默认：$user.home/store/config/delayOffset.json
     * @param rootDir
     * @return
     */
    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }


    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "statetable";
    }

    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }

}
