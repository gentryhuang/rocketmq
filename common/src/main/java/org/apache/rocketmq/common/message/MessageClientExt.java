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
package org.apache.rocketmq.common.message;

/**
 *
 */
public class MessageClientExt extends MessageExt {

    public String getOffsetMsgId() {
        return super.getMsgId();
    }

    public void setOffsetMsgId(String offsetMsgId) {
        super.setMsgId(offsetMsgId);
    }

    /**
     * 重写方法，返回的是我们通常所说的 msgId。全局ID 会存储在消息的属性 UNIQ_KEY
     * todo 如果消息消费失败需要重试（顺序消息不是重新投递到 Broker 的方式），RocketMQ 的做法是将消息重新发送到 Broker 服务器，此时全局 msgId 是不会发送变化的，但该消息的 offsetMsgId 会发送变化，因为其存储在服务器中的位置发生了变化。
     *
     * @return
     */
    @Override
    public String getMsgId() {
        String uniqID = MessageClientIDSetter.getUniqID(this);
        if (uniqID == null) {
            return this.getOffsetMsgId();
        } else {
            return uniqID;
        }
    }

    public void setMsgId(String msgId) {
        //DO NOTHING
        //MessageClientIDSetter.setUniqID(this);
    }
}
