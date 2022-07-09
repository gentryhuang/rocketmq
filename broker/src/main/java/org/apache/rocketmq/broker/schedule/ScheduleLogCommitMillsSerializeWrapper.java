package org.apache.rocketmq.broker.schedule;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ScheduleLogCommitMillsSerializeWrapper
 *
 * <p>
 * desc：
 */
public class ScheduleLogCommitMillsSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<Long, Long> scheduleLogCommitMillsTable = new ConcurrentHashMap<Long, Long>();

    public ConcurrentMap<Long, Long> getScheduleLogCommitMillsTable() {
        return scheduleLogCommitMillsTable;
    }

    public void setScheduleLogCommitMillsTable(ConcurrentMap<Long, Long> scheduleLogCommitMillsTable) {
        this.scheduleLogCommitMillsTable = scheduleLogCommitMillsTable;
    }
}
