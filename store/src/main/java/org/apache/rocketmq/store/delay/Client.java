package org.apache.rocketmq.test;

import io.netty.util.HashedWheelTimer;
import org.apache.rocketmq.store.delay.wheel.ScheduleTimeWheel;

import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * Client
 *
 * <p>
 * desc：
 */
public class Client {
    public static void main(String[] args) {
        long currentTimeMillis = System.currentTimeMillis();
        System.out.println(format2DateTime(currentTimeMillis));


        long l = currentTimeMillis - getEarlyMorningTimestamp();

        int num = (int) (l / (60 * 1000 * 30));
        System.out.println(num);

        long l1 = getLastTimestamp() - getEarlyMorningTimestamp() + 1;
        int result = (int) (l1 / (60 * 1000 * 30));
        System.out.println(result);

        System.out.println("--------------------------------");


        long l2 = System.currentTimeMillis();

        long granularityTimeMillis = getEarlyMorningTimestamp() + (int) ((l2 - getEarlyMorningTimestamp()) / (1000 * 60 * 30)) * (1000 * 60 * 30);

        String delayMessageStorePath = getDelayMessageStorePath(" $user.home/store/schedulelog", granularityTimeMillis);
        System.out.println(delayMessageStorePath);


        HashedWheelTimer hashedWheelTimer = ScheduleTimeWheel.INSTANCE.getWheelTimer();
        HashedWheelTimer hashedWheelTimer1 = ScheduleTimeWheel.INSTANCE.getWheelTimer();

        System.out.println(hashedWheelTimer == hashedWheelTimer1);


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
