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

import java.io.Serializable;

/**
 * 消息队列
 * 说明：
 * 1 每个 Topic 的队列分散在不同的 Broker 上，默认情况下 Topic 在 Broker 中对应 4 个写队列，4 个读队列
 * 2 在物理文件层面，只有写队列才会创建文件。
 * 例如；举个例子：写队列个数是8，设置的读队列个数是4.这个时候，会创建8个文件夹，代表0 1 2 3 4 5 6 7，但在消息消费时，路由信息只返回4，在具体拉取消息时，就只会消费0 1 2 3这4个队列中的消息，4 5 6 7中的信息压根就不会被消费。
 * 反过来，如果写队列个数是4，读队列个数是8，在生产消息时只会往0 1 2 3中生产消息，消费消息时则会从0 1 2 3 4 5 6 7所有的队列中消费，当然 4 5 6 7中压根就没有消息 ，假设消费group有两个消费者，事实上只有第一个消费者在真正的消费消息(0 1 2 3)，第二个消费者压根就消费不到消息。
 * 3 由此可见，只有readQueueNums>=writeQueueNums,程序才能正常进行。最佳实践是readQueueNums=writeQueueNums
 * 4 rocketmq设置读写队列数的目的在于方便队列的缩容和扩容。思考一个问题，一个topic在每个broker上创建了128个队列，现在需要将队列缩容到64个，怎么做才能100%不会丢失消息，并且无需重启应用程序？
 * - 最佳实践：先缩容写队列128->64，写队列由0 1 2 ......127缩至 0 1 2 ........63。等到64 65 66......127中的消息全部消费完后，再缩容读队列128->64.(同时缩容写队列和读队列可能会导致部分消息未被消费)
 */
public class MessageQueue implements Comparable<MessageQueue>, Serializable {
    private static final long serialVersionUID = 6191200464116433425L;
    /**
     * 消息队列对应的 Topic
     */
    private String topic;
    /**
     * Broker 名称
     */
    private String brokerName;
    /**
     * 消息队列 ID，是队列的序号
     */
    private int queueId;

    public MessageQueue() {

    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
    }

    /**
     * 注意，比较条件
     *
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MessageQueue other = (MessageQueue) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (queueId != other.queueId)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId + "]";
    }

    @Override
    public int compareTo(MessageQueue o) {
        {
            int result = this.topic.compareTo(o.topic);
            if (result != 0) {
                return result;
            }
        }

        {
            int result = this.brokerName.compareTo(o.brokerName);
            if (result != 0) {
                return result;
            }
        }

        return this.queueId - o.queueId;
    }
}
