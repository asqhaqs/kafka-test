package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.file.Worker;
import cn.situation.schedule.MonitorTask;
import cn.situation.util.*;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOG = LogUtil.getInstance(Main.class);

    private static long eventFileNamePosition = 0;
    private static Map<String, Long> metaFileNamePosition = new HashMap<>();
    private static long assetFileNamePosition = 0;

    private static SqliteUtil sqliteUtil = new SqliteUtil();

    static {
        Map<String, Long> positionMap = sqliteUtil.executeQuery(sqliteUtil.getQuerySql());
        LOG.info(String.format("[%s]: positionMap<%s>", "Main", positionMap));
        if (null != positionMap && positionMap.size() > 0) {
            eventFileNamePosition = positionMap.getOrDefault(SystemConstant.TYPE_EVENT, 0L);
            assetFileNamePosition = positionMap.getOrDefault(SystemConstant.TYPE_ASSET, 0L);
            String[] metaTypes = SystemConstant.TYPE_METADATA.split(",");
            for (String type : metaTypes) {
                metaFileNamePosition.put(type, positionMap.getOrDefault(type, 0L));
            }
        }
        if ("1".equals(SystemConstant.MONITOR_STATISTIC_ENABLED)) {
            SystemConstant.MONITOR_STATISTIC.put(SystemConstant.KIND_EVENT, 0L);
            SystemConstant.MONITOR_STATISTIC.put(SystemConstant.KIND_ASSET, 0L);
            SystemConstant.MONITOR_STATISTIC.put(SystemConstant.KIND_METADATA, 0L);
            String[] metaTypes = SystemConstant.TYPE_METADATA.split(",");
            for (String metaType : metaTypes) {
                SystemConstant.MONITOR_STATISTIC.put(metaType, 0L);
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
        for (int num = 0; num < threadNum; num++) {
            Worker worker = new Worker(context);
            new Thread(worker).start();
        }

        if ("1".equals(SystemConstant.MONITOR_STATISTIC_ENABLED)) {
            ScheduledExecutorService executorService = Executors.
                    newScheduledThreadPool(Integer.parseInt(SystemConstant.SCHEDULE_CORE_POOL_SIZE));
            executorService.scheduleWithFixedDelay(new MonitorTask(), 0,
                    Integer.parseInt(SystemConstant.MONITOR_PERIOD_SECONDS), TimeUnit.SECONDS);
        }

        LOG.info(String.format("[%s]: message<%s>", "main", "start work..."));
        SFTPUtil sftpUtil = new SFTPUtil();
        while (!Thread.currentThread ().isInterrupted ()) {
            Map<String, Long> map = new HashMap<>();
            try {
                if ("1".equals(SystemConstant.IF_DOWNLOAD_METADATA)) {
                    String[] metaDris = SystemConstant.METAdDATA_DIR.split(",");
                    String[] metaTypes = SystemConstant.TYPE_METADATA.split(",");
                    int size = metaDris.length;
                    for (int i = 0; i < size; i++) {
                        Map<String, Long> fileNameMap = sftpUtil.getRemoteFileName(metaDris[i],
                                metaTypes[i], SystemConstant.PACKAGE_SUFFIX, metaFileNamePosition.get(metaTypes[i]),
                                SystemConstant.KIND_METADATA, metaTypes[i]);
                        if (null != fileNameMap && !fileNameMap.isEmpty()) {
                            map.putAll(fileNameMap);
                        }
                    }
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_EVENT)) {
                    Map<String, Long> fileNameMap = sftpUtil.getRemoteFileName(SystemConstant.EVENT_DIR,
                            SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, eventFileNamePosition,
                            SystemConstant.KIND_EVENT, SystemConstant.TYPE_EVENT);
                    if (null != fileNameMap && !fileNameMap.isEmpty()) {
                        map.putAll(fileNameMap);
                    }
                }
                if ("1".equals(SystemConstant.IF_DOWNLOAD_ASSET)) {
                    Map<String, Long> fileNameMap = sftpUtil.getRemoteFileName(SystemConstant.ASSET_DIR,
                            SystemConstant.ASSET_PREFIX, SystemConstant.PACKAGE_SUFFIX, assetFileNamePosition,
                            SystemConstant.KIND_ASSET, SystemConstant.TYPE_ASSET);
                    if (null != fileNameMap && !fileNameMap.isEmpty()) {
                        map.putAll(fileNameMap);
                    }
                }
                List<Map.Entry<String,Long>> mapList = new ArrayList<>(map.entrySet());
                mapList.sort(new Comparator<Map.Entry<String,Long>>() {
                    public int compare(Map.Entry<String, Long> o1,
                                       Map.Entry<String, Long> o2) {
                        return o1.getValue().compareTo(o2.getValue());
                    }
                });
                LOG.debug(String.format("[%s]: mapList<%s>", "main", mapList));
                for (Map.Entry<String,Long> en : mapList) {
                    sender.send(en.getKey(), 0);
                    JSONObject obj = JSONObject.fromObject(en.getKey());
                    String fileName = obj.getString("fileName");
                    String kind = obj.getString("kind");
                    String type = obj.getString("type");
                    if (SystemConstant.KIND_EVENT.equals(kind)) {
                        eventFileNamePosition = FileUtil.getPositionByFileName(fileName);
                    } else if (SystemConstant.KIND_ASSET.equals(kind)) {
                        assetFileNamePosition = FileUtil.getPositionByFileName(fileName);
                    } else {
                        metaFileNamePosition.put(type, FileUtil.getPositionByFileName(fileName));
                    }
                }
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXEC_SLEEP_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXCEPT_SLEEP_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        sender.close();
        context.term();
    }
}
