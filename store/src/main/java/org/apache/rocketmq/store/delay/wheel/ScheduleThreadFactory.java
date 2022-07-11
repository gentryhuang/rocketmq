package org.apache.rocketmq.store.delay.wheel;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ScheduleThreadFactory
 * <p>
 * desc：
 */
public class ScheduleThreadFactory implements ThreadFactory {
    private AtomicInteger count = new AtomicInteger(0);
    private String scheduleLogPath;

    public ScheduleThreadFactory() {

    }

    public ScheduleThreadFactory(String scheduleLogPath) {
        this.scheduleLogPath = scheduleLogPath;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(this.scheduleLogPath + " - " + count.incrementAndGet());
        return t;
    }
}
