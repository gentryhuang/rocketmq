package org.apache.rocketmq.common.schedule.tool;

import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * ScheduleConfigHelper
 * <p>
 * desc：
 *
 * @author gentryhuang
 */
public class ScheduleConfigHelper {
    /**
     * 默认 30 分钟一个时间分区文件夹
     */
    public static final long TIME_GRANULARITY = 1000 * 60 * 30L;
    /**
     * 小于等于 5 分钟的消息加载到时间轮
     */
    public static final long TRIGGER_TIME = 1000 * 60 * 5;
    /**
     * 补偿粒度时间，过期的消息超过 5 分钟不再投递，打印可能是没有投递的消息日志
     */
    public static final long COMPENSATE_TIME = -1000 * 60 * 5;
    /**
     * 拉取消息要增加一个时间点，以及时拉取下一个延时分区文件到内存时间轮
     */
    public static final long ADD_DELAY_GRANULARITY = 1000 * 60;
    /**
     * 扫描分区时间文件的补偿粒度，为了精准控制和添加消息时的时间差，这里精确到 3s
     */
    public static final long DIFF_DELAY_GRANULARITY = 1000 * 3;
    /**
     * 清理文件的补偿时间，10分钟
     */
    public static final long CLEAN_SCHEDULE_FILE_GRANULARITY = 1000 * 60 * 10;


    /**
     * 获取当前延时时间属于哪个时间分区
     *
     * @param delayTimeMills 延时时间
     * @return
     */
    public static Long getDelayPartitionDirectory(Long delayTimeMills) {
        if (delayTimeMills == null || delayTimeMills < 0) {
            throw new NumberFormatException("delayTimeMills is illegal!");
        }
        return getTimePartitionDirectory(delayTimeMills);
    }

    /**
     * 延时消息按照到期时间划分的文件夹名称，如半小时为一个区间，该值为区间的起始时间戳
     *
     * @param time time
     * @return 时间分区文件夹
     */
    private static Long getTimePartitionDirectory(long time) {
        return getEarlyMorningTimestamp() + (int) ((time - getEarlyMorningTimestamp()) / (TIME_GRANULARITY)) * (TIME_GRANULARITY);
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
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String getCurrentDateTime() {
        long toEpochMilli = LocalDateTime.of(LocalDate.now(), LocalTime.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return format2DateTime(toEpochMilli);
    }

    public static void main(String[] args) {
       /*
                1.  2022-07-10 07:00:30   - 1657407630000L
          2.  2022-07-10 07:29:30   - 1657409370000L
          3.  2022-07-10 07:30:30   - 1657409430000L
          4.  2022-07-10 07:31:00   - 1657409460000L
          5.  2022-07-10 08:01:00   - 1657411260000L
          6.  2022-07-10 08:31:00   - 1657413060000L
          7.  2022-07-09 22:55:00   - 1657378500000L
          8.  2022-07-10 00:00:00   - 1657382400000L
        */
        System.out.println("2022-07-10 07:00:30 " + getDelayPartitionDirectory(1657407630000L));
        System.out.println("2022-07-10 07:29:30 " + getDelayPartitionDirectory(1657409370000L));
        System.out.println("2022-07-10 07:30:30 " + getDelayPartitionDirectory(1657409430000L));
        System.out.println("2022-07-10 07:31:00 " + getDelayPartitionDirectory(1657409460000L));


        System.out.println("2022-07-10 08:01:00 " + getDelayPartitionDirectory(1657411260000L));
        System.out.println("2022-07-09 22:55:00 " + getDelayPartitionDirectory(1657378500000L));
        System.out.println("2022-07-10 00:00:00 " + getDelayPartitionDirectory(1657382400000L));


    }
}
