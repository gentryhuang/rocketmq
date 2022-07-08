package org.apache.rocketmq.store.delay.tool;

import org.apache.commons.lang3.AnnotationUtils;

import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * DirConfigHelper
 * <p>
 * desc：
 */
public class DirConfigHelper {

    /**
     * 默认 30 分钟一个消息文件
     */
    public static final long TIME_GRANULARITY = 1000 * 60 * 30L;

    public static final String DELAY_TIME = "DELAY_TIME";

    /**
     * 获取当前延时时间属于哪个文件夹
     *
     * @param timeMills
     * @return
     */
    public static final Long getDirNameByMills(Long timeMills) {
        if (timeMills == null) {
            return null;
        }

        return getEarlyMorningTimestamp() + (int) ((timeMills - getEarlyMorningTimestamp()) / (TIME_GRANULARITY)) * (TIME_GRANULARITY);
    }


    /**
     * 延时消息按照到期时间划分的文件夹名称，如半小时为一个区间，该值为区间的起始时间戳
     * <p>
     * granularityTimeMillis 计算方式如下：
     * 延时时间的凌晨时间戳 + (int)((延时时间戳 - 凌晨时间戳) / 区间时间的毫秒值)) *  区间时间的毫秒值
     *
     * @param rootDir               $user.home/store/schedulelog
     * @param granularityTimeMillis 一定时间内的时间戳，如半小时为一个区间。
     */
    public static String getDelayMessageStorePath(final String rootDir, final long granularityTimeMillis) {
        return rootDir + File.separator + granularityTimeMillis;
    }

    /**
     * 获取今天零点时间戳
     */
    public static long getEarlyMorningTimestamp() {
        return LocalDateTime.of(LocalDate.now(), LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 获取今天零点时间戳
     */
    public static long getLastTimestamp() {
        return LocalDateTime.of(LocalDate.now(), LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 根据时间戳格式化为 yyyy-MM-dd HH:mm 格式
     *
     * @param timeMillis
     * @return
     */
    public static String format2DateTime(long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atZone(ZoneId.systemDefault()).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    /**
     * yyyy-MM-dd HH:mm
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

}
