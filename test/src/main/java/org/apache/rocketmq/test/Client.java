package org.apache.rocketmq.test;

import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * Client
 *
 * @author <a href="mailto:libao.huang@yunhutech.com">shunhua</a>
 * @since 2021/11/05
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
