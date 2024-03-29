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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 拉取消息结果
 */
public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<SelectMappedBufferResult>(100);

    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    /**
     * 拉取消息的状态
     */
    private GetMessageStatus status;

    /**
     * 消费者下次拉取消息的逻辑偏移量（类似下标），即消费者对 consumeQueue 的消费进度
     *
     * 是消费者在下一轮消息拉取时 offset 的重要依据，无论当次拉取的消息消费是否正常，nextBeginOffset都不会回滚，
     * 这是因为rocketMQ对消费异常的消息的处理是将消息重新发回broker端的重试队列（会为每个topic创建一个重试队列，以%RERTY%开头），达到重试时间后将消息投递到重试队列中进行消费重试。
     *
     */
    private long nextBeginOffset;
    /**
     * 消费队列 consumeQueue 记录的最小 offset 信息
     */
    private long minOffset;
    /**
     * 消费队列 consumeQueue 记录的最大 offset 信息
     */
    private long maxOffset;

    /**
     * 获取到的消息总大小
     */
    private int bufferTotalSize = 0;

    /**
     * 是否建议下次从从服务器拉取消息
     * todo 一般针对消费缓慢
     */
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        // 加入到 CommitLog 消息集合
        this.messageMapedList.add(mapedBuffer);
        // 加入到消息集合中
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        // 累加消息总大小
        this.bufferTotalSize += mapedBuffer.getSize();

        this.msgCount4Commercial += (int) Math.ceil(
                mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
                + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
                + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
