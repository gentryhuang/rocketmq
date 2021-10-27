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
package org.apache.rocketmq.common;

import java.io.IOException;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 配置管理对象的父类，提供公共的加载文件到内存和持久化缓存数据到文件的功能：
 * 1 加载文件内容到缓存中
 * 2 持久化内存的配置信息到文件
 * 具体实现类列举：
 * 1 订阅配置 SubscriptionGroupManager
 * 2 Topic 配置 TopicConfigManager
 * 3 延迟消息相关配置 ScheduleMessageService
 * 4 消费进度管理陪配置 ConsumerOffsetManager
 * 附加说明：
 * 这里内存到文件的信息，会有专门的后台线程刷新到文件，
 */
public abstract class ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 编码内容
     *
     * @return 编码后的内容
     */
    public abstract String encode();

    /**
     * 加载文件
     *
     * @return 加载是否成功
     */
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            // 加载文件到内存
            String jsonString = MixAll.file2String(fileName);

            // 如果内容不存在，则加载备份文件
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    /**
     * 配置文件地址
     *
     * @return
     */
    public abstract String configFilePath();

    /**
     * 加载备份文件
     *
     * @return
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    /**
     * 解码内容
     *
     * @param jsonString 内容
     */
    public abstract void decode(final String jsonString);

    /**
     * 持久化内存的配置信息到文件
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                // 写入文件
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    /**
     * 编码存储内容
     * todo 由具体配置实现类实现该方法。主要是将缓存数据序列化为 JSON 串，为后续写入文件做准备
     *
     * @param prettyFormat 是否格式化
     * @return 内容
     */
    public abstract String encode(final boolean prettyFormat);
}
