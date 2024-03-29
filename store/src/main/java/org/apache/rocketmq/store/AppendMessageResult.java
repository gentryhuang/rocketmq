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
package org.apache.rocketmq.store;

/**
 * 消息写入到 MappedFile （内存映射文件中，bytebuffer） 中的结果
 * When write a message to the commit log, returns results
 */
public class AppendMessageResult {

    /**
     * 消息追加结果
     */
    private AppendMessageStatus status;

    /**
     * 消息的物理偏移量，即当前消息是从哪里开始写的
     * Where to start writing
     */
    private long wroteOffset;

    // 写入字节总长度
    // Write Bytes
    private int wroteBytes;

    // todo 消息 offsetMsgId ,在 RocketMQ中，只需要提供 offsetMsgId，可用不必知道该消息所属的topic信息即可查询该条消息的内容。
    // Message ID
    private String msgId;

    // 消息存储时间，也就是写入到 MappedFile 中的时间。
    // Message storage timestamp
    private long storeTimestamp;

    /**
     * todo 消息消费队列的逻辑偏移量，类似数组下标
     * Consume queue's offset(step by one)
     */
    private long logicsOffset;

    /**
     * 写入到页存储的响应时间
     */
    private long pagecacheRT = 0;

    /**
     * 批量发消息时的消息条数
     */
    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    /**
     * @param status         追加结果：成功、到达文件尾（文件剩余空间不足）、消息长度超过、消息属性长度超出、未知错误
     * @param wroteOffset    消息的偏移量（相对于整个 commitlog 组）
     * @param wroteBytes     消息待写入字节
     * @param msgId          消息id
     * @param storeTimestamp 消息写入时间戳
     * @param logicsOffset   消息队列逻辑偏移量
     * @param pagecacheRT    消息写入时机戳（消息存储时间戳--- 消息存储开始时间戳）
     */
    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
                               long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(final long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                ", msgId='" + msgId + '\'' +
                ", storeTimestamp=" + storeTimestamp +
                ", logicsOffset=" + logicsOffset +
                ", pagecacheRT=" + pagecacheRT +
                ", msgNum=" + msgNum +
                '}';
    }
}
