package org.apache.rocketmq.broker.schedule;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.store.delay.ScheduleLogManager;

import java.io.File;

/**
 * ScheduleLogCommtMillsManager
 *
 * @author <a href="mailto:libao.huang@yunhutech.com">shunhua</a>
 * @since 2022/07/09
 * <p>
 * desc：
 */
public class ScheduleLogCommitMillsManager extends ConfigManager {
    private ScheduleLogManager scheduleLogManager;

    public ScheduleLogCommitMillsManager(ScheduleLogManager scheduleLogManager) {
        this.scheduleLogManager = scheduleLogManager;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        String rootDir = System.getProperty("user.home") + File.separator + "store";
        return rootDir + File.separator + "config" + File.separator + "scheduleCommitDelayTime.json";
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ScheduleLogCommitMillsSerializeWrapper scheduleLogCommitMillsSerializeWrapper =
                    ScheduleLogCommitMillsSerializeWrapper.fromJson(jsonString, ScheduleLogCommitMillsSerializeWrapper.class);

            if (scheduleLogCommitMillsSerializeWrapper != null) {
                scheduleLogManager.getScheduleDelayTimeTable().putAll(scheduleLogCommitMillsSerializeWrapper.getScheduleLogCommitMillsTable());
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        ScheduleLogCommitMillsSerializeWrapper topicConfigSerializeWrapper = new ScheduleLogCommitMillsSerializeWrapper();
        topicConfigSerializeWrapper.setScheduleLogCommitMillsTable(scheduleLogManager.getScheduleDelayTimeTable());
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

}
