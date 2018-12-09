package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.file.Worker;
import cn.situation.util.*;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final Logger LOG = LogUtil.getInstance(Main.class);

    private static int eventFileNamePosition = 0;
    private static Map<String, Integer> metaFileNamePosition = new HashMap<>();
    private static int assetFileNamePosition = 0;

    private static SqliteUtil sqliteUtil = new SqliteUtil();

    static {
        Map<String, Integer> positionMap = sqliteUtil.executeQuery(sqliteUtil.getQuerySql());
        LOG.info(String.format("[%s]: positionMap<%s>", "Main", positionMap));
        if (null != positionMap && positionMap.size() > 0) {
            eventFileNamePosition = positionMap.getOrDefault(SystemConstant.TYPE_EVENT, 0);
            assetFileNamePosition = positionMap.getOrDefault(SystemConstant.TYPE_ASSET, 0);
            String[] metaTypes = SystemConstant.TYPE_METADATA.split(",");
            for (String type : metaTypes) {
                metaFileNamePosition.put(type, positionMap.getOrDefault(type, 0));
            }
        }
        FileUtil.createDir(SystemConstant.LOCAL_FILE_DIR);
    }

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

        LOG.info(String.format("[%s]: message<%s>", "main", "start work..."));
        SFTPUtil sftpUtil = new SFTPUtil();
        while (!Thread.currentThread ().isInterrupted ()) {
            try {
                if ("1".equals(SystemConstant.IF_DOWNLOAD_EVENT)) {
                    List<String> fileNameList = sftpUtil.getRemoteFileName(SystemConstant.EVENT_DIR,
                            SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, eventFileNamePosition);
                    for (String fileName : fileNameList) {
                        sender.send(JsonUtil.pack2Json(SystemConstant.EVENT_DIR, fileName, SystemConstant.KIND_EVENT,
                                SystemConstant.TYPE_EVENT), 0);
                        eventFileNamePosition = FileUtil.getPositionByFileName(fileName);
                    }
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_METADATA)) {
                    String[] metaDris = SystemConstant.METAdDATA_DIR.split(",");
                    String[] metaTypes = SystemConstant.TYPE_METADATA.split(",");
                    int size = metaDris.length;
                    for (int i = 0; i < size; i++) {
                        List<String> fileNameList = sftpUtil.getRemoteFileName(metaDris[i],
                                metaTypes[i], SystemConstant.PACKAGE_SUFFIX, metaFileNamePosition.get(metaTypes[i]));
                        for (String fileName : fileNameList) {
                            sender.send(JsonUtil.pack2Json(metaDris[i], fileName, SystemConstant.KIND_METADATA,
                                    metaTypes[i]), 0);
                            metaFileNamePosition.put(metaTypes[i], FileUtil.getPositionByFileName(fileName));
                        }
                    }
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_ASSET)) {
                    List<String> fileNameList = sftpUtil.getRemoteFileName(SystemConstant.ASSET_DIR,
                            SystemConstant.ASSET_PREFIX, SystemConstant.PACKAGE_SUFFIX, assetFileNamePosition);
                    for (String fileName : fileNameList) {
                        sender.send(JsonUtil.pack2Json(SystemConstant.ASSET_DIR, fileName, SystemConstant.KIND_ASSET,
                                SystemConstant.TYPE_ASSET), 0);
                        assetFileNamePosition = FileUtil.getPositionByFileName(fileName);
                    }
                }
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXEC_INTERVAL_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXCEPT_INTERVAL_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        DicUtil.closePool();
        sender.close();
        context.term();
    }
}
