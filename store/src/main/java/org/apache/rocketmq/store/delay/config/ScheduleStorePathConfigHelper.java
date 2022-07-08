package org.apache.rocketmq.store.delay.config;

import java.io.File;

/**
 * DelayStorePathConfigHelper
 *
 * @author <a href="mailto:libao.huang@yunhutech.com">shunhua</a>
 * @since 2022/07/08
 * <p>
 * desc：
 */
public class ScheduleStorePathConfigHelper {

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
     * 获取延时消息不同分区时间下的最新投递时间
     *
     * @param rootDir
     * @return
     */
    public static String getScheduleLogTimeStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "schedulelogtime.json";
    }
}
