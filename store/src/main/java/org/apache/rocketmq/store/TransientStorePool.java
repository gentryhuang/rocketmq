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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 堆外内存池
 * <p>
 * 1. Java NIO的内存映射机制，提供了将文件系统中的文件映射到内存机制，实现对文件的操作转换对内存地址的操作，极大的提高了IO特性，但这部分内存并不是常驻内存，可能被置换到交换区；
 * 2. RocketMQ 为了提高消息发送的性能，引入了内存锁定机制，即将最近需要操作的commitlog文件映射到内存，并提供 内存锁定功能，确保这些文件始终存在内存中，该机制的控制参数就是transientStorePoolEnable。
 * 3. 如果开启了transientStorePoolEnable，内存锁定机制，那是不是随着commitlog文件的不断增加，最终导致内存溢出？答案如下：
 * 3.1 TransientStorePool默认会初始化5个DirectByteBuffer(堆外内存)，并提供内存锁定功能，即这部分内存不会被置换，可以通过transientStorePoolSize参数控制。
 * 3.2 在消息写入消息时，首先从池子中获取一个 DirectByteBuffer 进行消息的追加，当 DirectByteBuffer 写满后会重用。此外，同一时间，只会对一个commitlog文件进行顺序写，写完一个后，继续创建一个新的commitlog文件。
 * 故TransientStorePool的设计思想是循环利用这5个DirectByteBuffer，只需要写入到DirectByteBuffer的内容被提交到PageCache后，即可重复利用。
 * 3.3 因此，可以看出不会随着消息的不断写入而导致内存溢出。
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * availableBuffers 个数，可在 Broker 配置文件中进行配置，默认为 5
     */
    private final int poolSize;
    /**
     * 每个 ByteBuffer 的大小，默认 mappedFileSizeCommitLog = 1024 * 1024 * 1024 = 1G
     * 可以看出，TransientStorePool 为 CommitLog 文件服务
     */
    private final int fileSize;
    /**
     * 双端队列，ByteBuffer 容器
     */
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        /**
         * 创建数量为 pollSize ，默认为 5 个对外内存
         */
        for (int i = 0; i < poolSize; i++) {
            // 分配 DirectByteBuffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);

            // todo 利用 com.sun.jna 类库锁定该内存，避免被置换到交换区，以便提高存储性能
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            // 加入到缓存
            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        // 遍历堆外内存 DirectByteBuffer
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // todo 解锁堆外内存
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 归还 ByteBuffer
     *
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 获取可用的 ByteBuffer
     *
     * @return
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 获取可用堆外内存 ByteBuffer 个数
     *
     * @return
     */
    public int availableBufferNums() {
        // 是否启用transientStorePoolEnable机制
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
