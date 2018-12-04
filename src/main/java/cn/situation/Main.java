package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.file.Worker;
import cn.situation.util.LogUtil;
import cn.situation.util.SFTPUtil;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {

    private static final Logger LOG = LogUtil.getInstance(Main.class);

    private static List<String> eventFileNameList = new ArrayList<>();
    private static List<String> metaFileNameList = new ArrayList<>();
    private static List<String> assertFileNameList = new ArrayList<>();
    private static SFTPUtil sftpUtil = new SFTPUtil();

    public static void main(String[] args) {

        ZMQ.Context context = ZMQ.context(0);

        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.bind("inproc://workers");
        sender.setHWM(Integer.valueOf(SystemConstant.ZMQ_SNDHWM));

        int threadNum = Integer.valueOf(SystemConstant.WORKER_THREAD_NUM);
        for(int num = 0; num < threadNum; num++) {
            Worker worker = new Worker(context);
            new Thread(worker).start();
        }

        LOG.error(String.format("[%s]: message<%s>", "main", "start work..."));

        while (!Thread.currentThread ().isInterrupted ()) {

            try {
                if ("1".equals(SystemConstant.IF_DOWNLOAD_EVENT)) {
                    List<String> fileNameList = sftpUtil.getRemoteFileName(SystemConstant.EVENT_DIR,
                            "Event_detection", SystemConstant.PACKAGE_SUFFIX, eventFileNameList);
                    for (String fileName : fileNameList) {
                        JSONObject json = new JSONObject();
                        json.put("filePath", SystemConstant.EVENT_DIR);
                        json.put("fileName", fileName);
                        json.put("kind", "event"); // 种类
                        json.put("type", "event"); // 分类
                        sender.send(json.toString(), 0);
                    }
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_METADATA)) {
                    //TODO
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_ASSERT)) {
                    //TODO
                }
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXEC_INTERVAL_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        sender.close();
        context.term();
    }
}
