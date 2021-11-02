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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 映射文件队列，也就是 MappedFile 的管理容器，对存储目录进行封装，封装的目录下会存在多个内存映射文件 MappedFile 。对上层提供可无限使用的文件容量。
 * 说明：
 * 1 MappedFile 代表一个个物理文件，而 MappedFileQueue 代表由一个个 MappedFile 组成的一个连续逻辑的大文件。
 * 2 每一个 MappedFile 的命名为该文件中第一条数据在整个文件序列（映射文件队列）中的物理偏移量。
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 存储目录
     */
    private final String storePath;
    /**
     * 单个 MappedFile 文件大小
     */
    private final int mappedFileSize;
    /**
     * mappedFile 集合
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 创建 MappedFile 的线程任务，主要起到预分配 MappedFile 的作用
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 当前刷盘指针，表示该指针之前的所有数据全部持久化到磁盘。针对该 MappedFileQueue
     */
    private long flushedWhere = 0;

    /**
     * 当前数据提交指针，该值大于、等于 flushedWhere
     * commit (已提交)位置，针对该 MappedFileQueue
     */
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    /**
     * 构造方法
     *
     * @param storePath                 存储目录
     * @param mappedFileSize            单个 MappedFile 的大小
     * @param allocateMappedFileService 创建 MappedFile 的线程任务，主要起到预分配 MappedFile 的作用
     */
    public MappedFileQueue(final String storePath,
                           int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /*--------------------- 不同维度查找 MappedFile 的方法 ------------------------*/


    /**
     * 根据消息存储时间戳查找 MappedFile
     *
     * @param timestamp 待查找时间戳
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs)
            return null;

        // 从 MappedFile 列表中的第一个文件开始查找
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];

            // 判断 MappedFile 最后一次更新时间 和 待查找时间戳的大小，修改时间大于等于待查找时间戳则返回
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        // 没有找到，则返回最后一个 MappedFile
        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }


    /**
     * 根据偏移量查找 MappedFile
     *
     * @param offset 物理偏移量
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 根据消息偏移量 offset 查找 MappedFile
     * 说明：
     * 1 使用内存映射，只要是存在于存储目录下的文件，都需要对应创建内存映射文件
     * 2 如果不定时将已消费的消息从存储文件中删除，会造成极大的内存压力于资源浪费，因此 RocketMQ 采取定时删除存储文件的策略
     * 3 在存储目录中，第一个文件不一定是 00000000000000000000 ，因为该文件在某一时刻会被删除
     *
     * @param offset                物理偏移量
     * @param returnFirstOnNotFound 如果没有找到，是否返回第一个
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {

            // 获取第一个 MappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 获取最后一个 MappedFile
            MappedFile lastMappedFile = this.getLastMappedFile();

            // 存在 MappedFile
            if (firstMappedFile != null && lastMappedFile != null) {
                // 偏移量不在文件中
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());

                    // 偏移量在某个文件中
                } else {
                    /**
                     * todo 定位当前 offset 位于第几个物理文件
                     * 注意：这里不能直接使用 offset/this.mappedFileSize，因为 RocketMQ 可能将文件名靠前的删除了，这样的话得到的文件下标就不准确了。
                     * 比如：通过 offset/this.mappedFileSize = 2，但是 00000000000000000000 文件已经被删除了，这个时候得到的 2 就不能作为正确的下标了，
                     *       必须通过计算：(int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize)) 得到正确文件所在的下标，即
                     *       todo (消息物理偏移量 - 第一个MappedFile 的起始偏移量）/文件固定大小 即可得到所在文件下标
                     */
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        // 取出 offset 所在物理文件
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    // 偏移量在文件偏移量范围内，找到了对应的文件
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    // 通过 offset 没有计算得到目标 MappedFile ，这里使用兜底方案，遍历所有 MappedFile ，然后依次判断给定的偏移量 offset 在不在对应 MappedFile 中
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                // 确实没有对应的 MappedFile ，如果要求返回第一个 MappedFile ，则直接返回
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }


    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期内存文件
     * 注意：会有专门的后台线程定时将内存文件刷到磁盘文件
     *
     * @param files
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }
            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 按照顺序创建 MappedFile，即从磁盘加载文件
     *
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);

        // 获取 CommitLog 文件列表
        File[] files = dir.listFiles();
        if (files != null) {
            // 按照文件名进行排序
            // ascending order
            Arrays.sort(files);

            // 遍历文件
            for (File file : files) {
                // 如果文件与配置文件的单个文件大小不一致，则忽略
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    // 还原
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    // 将 wrotePosition、flushedPosition、committedPosition 三个指针都设置为文件大小
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);

                    // 加入缓存
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取 MappedFile
     * 规则：先尝试获取 MappedFile，没有获取到才会根据传入的 startOffset 创建。
     *
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 创建文件开始offset。-1时，不创建
        long createOffset = -1;
        // 最后一个 MappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        // 一个映射文件都不存在
        if (mappedFileLast == null) {
            // 根据传入的起始偏移量，计算出下一个文件名称
            // 方式：startOffset - 基于文件大小的余数，得到文件名，也就是文件名以存储在当前文件的第一个数据在整个文件组中的偏移量命名的，第一个文件名为 0
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        // 最后一个文件已满，计算出下一个文件名
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 创建文件
        if (createOffset != -1 && needCreate) {

            // 计算文件名，命名规则：
            // fileName[n] = fileName[n - 1] + n * mappedFileSize
            // fileName[0] = startOffset - (startOffset % this.mappedFileSize)
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);


            MappedFile mappedFile = null;
            // 预分配文件 MappedFile
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            // 设置 MappedFile是否是第一个创建的文件
            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }

                // 文件缓存起来
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 根据起始偏移量获取最后的 MappedFile，没有则创建一个
     *
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个 MappedFile
     *
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取存储文件最小偏移量
     *
     * @return
     */
    public long getMinOffset() {
        if (!this.mappedFiles.isEmpty()) {
            try {
                // 注意，并发直接返回 0 ，而是返回 MappedFile 的 getFileFromOffset()
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取存储文件的最大偏移量。返回最后一个 MappedFile 的 fileFromOffset ，加上该 MappedFile 当前的读指针
     *
     * @return
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 最后一个文件的起始偏移量 + 读位置
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取存储文件最大的写位置
     *
     * @return
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 最后一个文件的起始偏移量 + 写位置
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 根据时间执行文件销毁和删除
     *
     * @param expiredTime
     * @param deleteFilesInterval
     * @param intervalForcibly
     * @param cleanImmediately
     * @return
     */
    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        // 到倒数第二个文件
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];

                // 取文件的修改时间，计算文件最大存活时间。过期时间默认 72小时
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;

                // 如果当前时间大于文件的最大存活时间或须臾奥强制删除文件（当磁盘使用超过预定的阈值）
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {

                    // 删除内部封装的文件通道和物理文件
                    if (mappedFile.destroy(intervalForcibly)) {

                        // 将内存文件加入待删除文件列表中，最后统一清除内存文件
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        // 删除过期内存文件
        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 刷盘
     * 说明：
     * 1 根据上次刷盘偏移量，找到当前待刷盘 MappedFile 对象，最终执行 MappedByteBuffer.force() 或 FileChannel.force() 方法
     *
     * @param flushLeastPages
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;

        // 1 根据上次刷盘的位置，得到当前的 MappedFile 对象，没有则创建
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();

            // 2 执行 MappedFile 的 flush 方法
            int offset = mappedFile.flush(flushLeastPages);

            // 刷盘后，新的刷盘物理偏移量 where
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;

            // 3 更新刷新的位置
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交
     *
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;

        // 根据 committedWhere 找到具体的 MappedFile 文件
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {

            // 调用 MappedFile 的 commit 函数
            int offset = mappedFile.commit(commitLeastPages);

            // 更新 MappedFileQueue 提交的偏移量
            long where = mappedFile.getFileFromOffset() + offset;

            // 如果 result = true，说明本次 mappedFile 没有执行 commit 操作
            // 即 false 并不是代表提交失败，而是表示有数据提交成功了，true 表示没有数据提交成功
            result = where == this.committedWhere;

            // 更新提交指针
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 获取第一个 MappedFile
     *
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 获取内存文件大小
     *
     * @return
     */
    public long getMappedMemorySize() {
        long size = 0;
        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }
        return size;
    }

    /**
     * 尝试删除第一个文件
     *
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
