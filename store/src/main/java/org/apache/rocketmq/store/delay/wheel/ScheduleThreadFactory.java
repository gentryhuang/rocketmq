package org.apache.rocketmq.store.delay.wheel;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ScheduleThreadFactory
 *
 * desc：
 */
public class ScheduleThreadFactory implements ThreadFactory {
    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("schedule-" + count.incrementAndGet());
        return t;
    }
}
