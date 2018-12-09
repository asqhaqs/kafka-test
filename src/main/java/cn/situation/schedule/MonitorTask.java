package cn.situation.schedule;

import cn.situation.cons.SystemConstant;
import cn.situation.util.JsonUtil;
import cn.situation.util.LogUtil;
import net.sf.json.JSONObject;
import org.slf4j.Logger;

/**
 * @author lenzhao
 * @date 2018/12/9 21:27
 * @description TODO
 */
public class MonitorTask implements Runnable {

    private static final Logger LOG = LogUtil.getInstance(MonitorTask.class);

    public void run() {
        LOG.info(String.format("[%s]: static<%s>", "MonitorTask",
                JSONObject.fromObject(SystemConstant.MONITOR_STATISTIC).toString()));
    }
}
