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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * RocketMQ 内存映射文件的具体实现
 * 说明：
 * 1 存储文件名格式：00000000000000000000、00000000001073741824、00000000002147483648等文件
 * 2 每个 MappedFile 大小统一
 * 3 文件命名方式：fileName[n] = fileName[n - 1] + mappedFileSize
 */
public class MappedFile extends ReferenceResource {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 操作系统每页大小，默认 4KB
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;
    /**
     * 当前 JVM 实例中 MappedFile 的虚拟内存
     * 类变量，用于记录所有 MappedFile 实例已使用字节总数，根据文件大小计算的
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    /**
     * 当前 JVM 实例中 MappedFile 对象的个数
     * 类变量，用于记录MappedFile 个数，根据文件个数统计
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);


    /*-------------- 以下三个变量的区别与联系
     * wrotePosition: 写入到缓存映射文件 MappedFile 中的位置，不论是否使用堆外内存，写成功该指针都会移动
     * committedPosition：将数据写入到 FileChannel 中的位置
     * flushedPosition：将数据刷盘的位置
     * todo 特别说明：在 MappedFile 设计中，只有提交了的数据（写入 MappedByteBuffer 或 FileChannel 中的数据）才是安全的数据
     ---------------- */

    /**
     * 当前 MappedFile 对象 当前写入位置（从 0 开始，内存映射文件的写指针）
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 当前 MappedFile 对象 已提交位置（提交指针）
     * 注意：
     * 1 如果开启 transientStorePoolEnable ，则数据会存储在 TransientStorePool 中
     * 2 通过 commit 线程将数据提交到 FileChannel 中，最后通过 Flush 线程将数据持久化到磁盘。
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /**
     * 当前 MappedFile 对象 已刷盘位置，应满足：commitedPosition >= flushedPosition
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件大小，注意，是单个文件
     */
    protected int fileSize;
    /**
     * 文件名
     */
    private String fileName;
    /**
     * 物理文件
     */
    private File file;

    /**
     * 文件通道，JDK 中的 RandomAccessFile
     */
    protected FileChannel fileChannel;

    /**
     * 物理文件对应的内存映射 Buffer，对应操作系统的 PageCache
     *
     * java nio 中引入基于 MappedByteBuffer 操作大文件的方式，其读写性能极高
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 堆外内存 DirectByteBuffer
     * 说明：
     * 如果开启了 transientStorePoolEnable 内存锁定，数据会先写入堆外内存（DirectByteBuffer），然后提交到 MappedFile 创建的 FileChannel 中， 并最终刷写到磁盘。
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * DirectByteBuffer的缓冲池，即堆外内存池，transientStorePoolEnable 为 true 时生效。
     */
    protected TransientStorePool transientStorePool = null;


    /**
     * 该文件起始物理偏移量（对应了在 MappedFileQueue 中的偏移量）
     */
    private long fileFromOffset;

    /**
     * 最后一次写入内容的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是 MappedFileQueue 队列中第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 创建并初始化 MappedFile
     *
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 初始化 - 开启堆外内存
     *
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);

        // 从堆外内存池中取出并赋值 writeBuffer
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 初始化 - 没有开启堆外内存
     *
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        // 根据文件名创建一个文件
        this.file = new File(fileName);
        // 文件名作为 起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());

        boolean ok = false;

        // 文件目录处理
        ensureDirOK(this.file.getParent());

        try {
            // 创建具有读写功能的通道，对文件 file 进行封装
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

            // todo 根据文件通道映射一个 MappedByteBuffer，将文件映射到内存中
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 将消息追加到 MappedFile
     *
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 将消息追加到当前 MappedFile
     *
     * @param messageExt
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 1 获取 MappedFile 当前写指针
        int currentPos = this.wrotePosition.get();

        // 文件还可以写
        if (currentPos < this.fileSize) {

            // todo 根据是否开启使用堆外内存，选择不同字节缓存区。这个很重要，后续刷盘不会刷堆外内存的数据，只会刷堆内存的数据，也就是说要想将堆外内存进行持久化，必须先提交
            // 获取写入字节缓冲区； slice() 方法用于创建一个与原对象共享的内存区，且拥有独立的 position、limit、capacity 等指针
            // 为什么会有 writeBuffer != null 的判断后，使用不同的字节缓冲区，见：FlushCommitLogService。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();

            // 设置 currentPos 为当前指针
            byteBuffer.position(currentPos);
            AppendMessageResult result;

            // 2 根据消息类型，是批量还是单个消息，进入相应的消息写入处理逻辑
            // 消息追加逻辑，通过调用传入的 AppendMessageCallback 实现
            if (messageExt instanceof MessageExtBrokerInner) {

                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);

            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            // 3 todo 更新写入指针
            // todo 注意，无论使用的是堆外内存，还是物理文件的内存映射，数据写入后，写入指针都是移动。
            // todo 注意在使用堆外内存的情况下，committedPosition 这个提交指针的意义，他并不是指写入到内存的指针，而是指提交到堆内存的指针。
            // todo 因此，在写入的时候 committedPosition < wrotePosition ，在提交的时候 committedPosition 才会追赶 wrotePosition
            this.wrotePosition.addAndGet(result.getWroteBytes());

            // 4 更新消息写入的时间
            this.storeTimestamp = result.getStoreTimestamp();

            return result;
        }

        // 如果写入指针大于或等于文件大小，表明文件已写满
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        // 还能写下
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 从当前写入位置写数据
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }

            // 更新写入位置
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘：将内存中的数据写入磁盘
     * 说明：
     * 0 flushedPosition 应该等于 MappedByteBuffer 中的指针
     * 1 刷写的逻辑就是调用 FileChannel 或 MappedByteBuffer 的 force 方法将数据写入磁盘
     * 2 考虑到写入性能，满足 flushLeastPages * OS_PAGE_SIZE 才进行 flush。
     *
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {

        // 判断是否可以刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                /**
                 * todo 刷盘
                 * 1 如果 writeBuffer 不为空，说明开启了使用堆外内存，那么对于刷盘来说，刷盘 flushedPosition 应该等于上一次提交指针 committedPosition，
                 * 因为上一次提交的数据就是进入 MappedByteBuffer(FileChannel) 中。注意，其实提交后 committedPosition 指针会更为为 wrotePosition。
                 * todo 只是刷盘的时候只能刷提交到堆内存的数据，因此不能直接使用 wrotePosition，在未提交之前 wrotePosition > committedPosition 的
                 *
                 * 2 如果 writeBuffer 为空，说明数据是直接进入 MappedByteBuffer(FileChannel)的，wrotePosition 代表的是 MappedByteBuffer 中的指针
                 */
                int value = getReadPosition();

                // 刷写的逻辑就是调用 FileChannel 或 MappedByteBuffer 的force 方法，将内存数据写入到磁盘
                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                // 更新刷盘位置为 value
                // todo
                //  1 对于使用堆外内存，刷盘位置更新为 committedPosition
                //  2 对于使用堆内存，刷盘位置更新为 wrotePosition
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }

        // 返回已刷盘位置
        return this.getFlushedPosition();
    }

    /**
     * 提交
     * <p>
     * 说明：考虑到写入性能，满足 commitLeastPages * OS_PAGE_SIZE 才进行 commit
     *
     * @param commitLeastPages 本次至少提交的页数，如果当前需要提交的数据所占的页数小于 commitLeastPages ，则不执行本次提交操作，等待下次提交。
     * @return
     */
    public int commit(final int commitLeastPages) {
        // 1 如果 writeBuffer 为空，则表示 IO 操作都是直接基于 FileChannel,所以此时返回当前可写的位置，作为committedPosition 即可
        // todo commit 操作的主体是 writeBuffer(即使用了堆外内存) ，否则就没有提交的必要
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }

        // 2 是否可提交
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 提交逻辑
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // todo 所有堆外数据提交完毕，归还堆外内存 writeBuffer
        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            // 归还后，重置该堆外内存
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        // 返回提交位置
        return this.committedPosition.get();
    }

    /**
     * commit 的作用是将 writeBuffer 中的数据写入到 FileChannel 中
     *
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        // 写入位置
        // todo 消息在写入成功后会更新 wrotePosition ，对于使用堆外内存的情况，在写入成功后，committedPosition < wrotePosition
        int writePos = this.wrotePosition.get();

        // 上次提交位置
        int lastCommittedPosition = this.committedPosition.get();

        // 写入堆外内存后，写入指针 wrotePosition 也会更新
        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {

                // 这里使用 slice 方法，主要是用的同一片内存空间，但单独的指针。
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 回退到上一次提交的位置 lastCommittedPosition
                byteBuffer.position(lastCommittedPosition);
                // todo 设置当前最大有效数据指针，即范围 lastCommittedPosition ～ writePos
                byteBuffer.limit(writePos);


                // 将 committedPosition 到 wrotePosition 的数据复制（写入）到 FileChannel 中，即将上一次 commitedPosition + 当前写位置这些数据全部写入到 FileChannel中
                // todo commit 的作用原来是将 writeBuffer 中的数据写入到 FileChannel 中。
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);

                // todo 更新 committedPosition 的位置，即写入到 FileChannel 后，更新提交指针 committedPosition 为 wrotePosition
                this.committedPosition.set(writePos);

            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够 flush ，满足如下条件任意条件：
     * 1. 映射文件已经写满
     * 2. flushLeastPages > 0 && 未 flush 页超过flushLeastPages
     * 3. flushLeastPages = 0 && 有新写入部分
     *
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 上次刷盘偏移量
        int flush = this.flushedPosition.get();
        // 当前写入
        int write = getReadPosition();

        // 如果文件已满，返回 true
        if (this.isFull()) {
            return true;
        }

        // 如果 flushLeastPages 大于0，则需要判断当前写入的偏移与上次刷新偏移量之间的间隔，如果超过commitLeastPages页数，则提交，否则本次不提交。
        if (flushLeastPages > 0) {
            // 计算待刷盘页，如果待刷盘页 >= flushLeastPages，则允许刷盘
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // flushLeastPages = 0 && 有新写入部分，则允许刷盘
        return write > flush;
    }

    /**
     * 是否能够 commit ，满足如下条件任意：
     * 1. 映射文件已经写满
     * 2. commitLeastPages > 0 && 未 commit 脏页数超过commitLeastPages
     * 3. commitLeastPages = 0 && 存在脏页
     *
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 提交位置
        int flush = this.committedPosition.get();
        // 写入位置
        int write = this.wrotePosition.get();

        // 1 如果文件写满了（写入堆外内存，写入指针 wrotePosition 也在移动），则允许提交
        if (this.isFull()) {
            return true;
        }

        // 2 脏页的数量 >= commitLeastPages ，则允许提交
        if (commitLeastPages > 0) {
            // 计算脏页的数量，如果脏页数量 >= commitLeastPages ，则允许提交
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        // 3 如果 commitLeastPages = 0，则表示只要存在脏页就提交
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 从 pos 偏移量读取 size 长度的内容
     *
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        // 获取当前 MappedFile 最大有效位置
        int readPosition = getReadPosition();

        // 读取的数据在有效范围内
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                // 从 pos 偏移量读取 size 长度的内容
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);


                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 根据偏移量获取对应范围内的数据，即从当前 MappedFile 传入偏移量开始读取  readPosition - pos 范围内的数据
     *
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取当前有效数据的最大位置
        int readPosition = getReadPosition();

        // 只有给定的 pos 小于最大有效位置才有意义
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {

                // 查找 pos 到当前最大可读指针之间的数据
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                // 从 pos 位置读取数据
                byteBuffer.position(pos);
                // 读取字节数为 size
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);

                // byteBufferNew 保存的就是目标数据，
                // todo  注意 startOffset 的值为 this.fileFromOffset + pos，即转为物理偏移量
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 销毁 MappedFile 封装的文件通道和物理文件
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        // 1 关闭 MappedFile 并尝试释放资源
        this.shutdown(intervalForcibly);

        // 2 判断是否清理完成，标准是引用次数 <= 0 & cleanupOver = true
        if (this.isCleanupOver()) {
            try {

                // 3 关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();

                // 4 删除物理文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 当前有效数据的最大位置
     *
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        /**
         * 1 如果 writeBuffer 为空，说明是直接写入MappedByteBuffer， 则直接返回当前的写指针
         * 2 如果 writeBuffer 不为空，则返回上一次提交的指针
         */
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
