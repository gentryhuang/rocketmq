package org.apache.rocketmq.store.delay.wheel;

import io.netty.util.HashedWheelTimer;

import java.util.concurrent.TimeUnit;

/**
 * SheduleTimeWheel
 * <p>
 * desc：
 */
public enum ScheduleTimeWheel {
    INSTANCE;

    private HashedWheelTimer wheelTimer;


    ScheduleTimeWheel() {
        wheelTimer = new HashedWheelTimer(
                new ScheduleThreadFactory(),
                1000,
                TimeUnit.MILLISECONDS, 64);
    }

    public HashedWheelTimer getWheelTimer() {
        return wheelTimer;
    }
}
