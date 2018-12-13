package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.support.service.MessageService;
import cn.situation.data.AssetTrans;
import cn.situation.data.EventTrans;
import cn.situation.util.*;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.zeromq.ZMQ;

public class Worker implements Runnable {

    private static final Logger LOG = LogUtil.getInstance(Worker.class);

    private static SqliteUtil sqliteUtil = SqliteUtil.getInstance();

    private SFTPUtil sftpUtil = new SFTPUtil();

    private ZMQ.Context context;

    public Worker (ZMQ.Context context) {
        this.context = context;
    }

    @Override
    public void run() {
        ZMQ.Socket socket = context.socket(ZMQ.PULL);
        socket.connect ("inproc://workers");
        socket.setHWM(Integer.valueOf(SystemConstant.ZMQ_RCVHWM));
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] result = socket.recv(0);
                LOG.info(String.format("[%s]: result<%s>, thread-id<%s>", "Worker.run", new String(result),
                        getId()));
                action(result);
            } catch (Exception e) {
                LOG.error("[%s]: message<%s>", "Worker.run", e.getMessage());
                LOG.error(e.getMessage(), e);
            }
        }
        socket.close();
    }

    private void action(byte[] data) {
        JSONObject json = JSONObject.fromObject(new String(data));
        String kind = json.getString("kind");
        String type = json.getString("type");
        String filePath = json.getString("filePath");
        String fileName = json.getString("fileName");
        LOG.info(String.format("[%s]: kind<%s>, type<%s>, filePath<%s>, fileName<%s>", "action",
                kind, type, filePath, fileName));
        try {
            if (SystemConstant.KIND_EVENT.equals(kind)) {
                handleEvent(filePath, fileName);
            }
            if (SystemConstant.KIND_METADATA.equals(kind)) {
                handleMetadata(type, filePath, fileName);
            }
            if (SystemConstant.KIND_ASSET.equals(kind)) {
                handleDevAssets(filePath, fileName);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if ("1".equals(SystemConstant.SQLITE_UPDATE_ENABLED)) {
                sqliteUtil.executeUpdate(sqliteUtil.getUpdateSql(kind, type, fileName));
            }
        }
    }

    private void handleMetadata(String type, String remotePath, String fileName) {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                type, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.debug(String.format("[%s]: type<%s>, remotePath<%s>, fileName<%s>, result<%s>", "handleMetadata",
                type, remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_METADATA);
            }
        }
    }

    private void handleEvent(String remotePath, String fileName) {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.debug(String.format("[%s]: remotePath<%s>, fileName<%s>, result<%s>",
                "handleEvent", remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_EVENT);
            }
        }
    }
    
    private void handleDevAssets(String remotePath, String fileName) {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                SystemConstant.ASSET_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.debug(String.format("[%s]: remotePath<%s>, fileName<%s>, result<%s>",
                "handleDevAssets", remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_ASSET);
            }
        }
    }

    private void execByLine(String filePath, String fileName, String kind) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile() || file.length() == 0) {
            return;
        }
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader reader = null;
        try {
            fileInputStream = new FileInputStream(file);
            inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader, Integer.parseInt(SystemConstant.INPUT_BUFFER_SIZE));
            String line;
            List<String> queue = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                LOG.debug(String.format("[%s]: line<%s>, kind<%s>, fileName<%s>", "execByLine", line, kind, fileName));
                if ("1".equals(SystemConstant.MONITOR_STATISTIC_ENABLED)) {
                    SystemConstant.MONITOR_STATISTIC.put(kind, (SystemConstant.MONITOR_STATISTIC.get(kind)+1));
                }
                queue.add(line);
                if ("1".equals(SystemConstant.QUEUE_CHECK_POLICY) && queue.size() < Integer.parseInt(SystemConstant.QUEUE_CHECK_INTERVAL_SIZE) &&
                        (System.currentTimeMillis() - startTime) < Integer.parseInt(SystemConstant.QUEUE_CHECK_INTERVAL_MS)) {
                    continue;
                }
                parseMsg(queue, kind, fileName);
                startTime = System.currentTimeMillis();
                queue.clear();
            }
            if (!queue.isEmpty()) {
                parseMsg(queue, kind, fileName);
                queue.clear();
            }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
            try {
                if (null != reader) {
                    reader.close();
                }
                if (null != inputStreamReader) {
                    inputStreamReader.close();
                }
                if (null != fileInputStream) {
                    fileInputStream.close();
                }
            } catch (Exception ie) {
                LOG.error(ie.getMessage(), ie);
            }
            FileUtil.delDir(file.getParent());
        }
    }

    private void parseMsg(List<String> dataList, String kind, String fileName) {
        LOG.debug(String.format("[%s]: kind<%s>, queueSize<%s>", "parseMsg", kind, dataList.size()));
        if (SystemConstant.KIND_METADATA.equals(kind)) {
            MessageService.parseMetadata(dataList, fileName);
        } else if (SystemConstant.KIND_EVENT.equals(kind)) {
            EventTrans.do_trans(dataList);
        } else if (SystemConstant.KIND_ASSET.equals(kind)) {
            AssetTrans.do_trans(dataList);
        }
    }
    
    private long getId() {
        return Thread.currentThread().getId();
    }
    
}
