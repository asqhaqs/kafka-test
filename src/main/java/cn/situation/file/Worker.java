package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.support.service.MessageService;
import cn.situation.data.AssetTrans;
import cn.situation.data.EventTrans;
import cn.situation.util.FileUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.SFTPUtil;
import cn.situation.util.SqliteUtil;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
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

    private void action(byte[] data) throws Exception {
        JSONObject json = JSONObject.fromObject(new String(data));
        String kind = json.getString("kind");
        String type = json.getString("type");
        String filePath = json.getString("filePath");
        String fileName = json.getString("fileName");
        LOG.info(String.format("[%s]: kind<%s>, type<%s>, filePath<%s>, fileName<%s>", "action",
                kind, type, filePath, fileName));
        if (SystemConstant.KIND_EVENT.equals(kind)) {
            handleEvent(filePath, fileName);
        }
        if (SystemConstant.KIND_METADATA.equals(kind)) {
            handleMetadata(type, filePath, fileName);
        }
        if (SystemConstant.KIND_ASSET.equals(kind)) {
            handleDevAssets(filePath, fileName);
        }
        if ("1".equals(SystemConstant.SQLITE_UPDATE_ENABLED)) {
            sqliteUtil.executeUpdate(sqliteUtil.getUpdateSql(kind, type, fileName));
        }
    }

    private void handleMetadata(String type, String remotePath, String fileName) throws Exception {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                type, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.info(String.format("[%s]: type<%s>, remotePath<%s>, fileName<%s>, result<%s>", "handleMetadata",
                type, remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_METADATA);
            }
        }
    }

    private void handleEvent(String remotePath, String fileName) throws Exception {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.info(String.format("[%s]: remotePath<%s>, fileName<%s>, result<%s>",
                "handleEvent", remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_EVENT);
            }
        }
    }
    
    private void handleDevAssets(String remotePath, String fileName) throws Exception {
        boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                SystemConstant.ASSET_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
        LOG.info(String.format("[%s]: remotePath<%s>, fileName<%s>, result<%s>",
                "handleDevAssets", remotePath, fileName, result));
        if (result) {
            List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
            for (File file : fileList) {
                execByLine(file.getAbsolutePath(), fileName, SystemConstant.KIND_ASSET);
            }
        }
    }

    private void execByLine(String filePath, String fileName, String kind) throws Exception {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile() || file.length() == 0) {
            return;
        }
        FileInputStream fileInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        BufferedReader reader = new BufferedReader(inputStreamReader, Integer.parseInt(SystemConstant.INPUT_BUFFER_SIZE));
        String line;
        while ((line = reader.readLine()) != null) {
            LOG.debug(String.format("[%s]: line<%s>, kind<%s>, fileName<%s>", "execByLine", line, kind, fileName));
            if ("1".equals(SystemConstant.MONITOR_STATISTIC_ENABLED)) {
                SystemConstant.MONITOR_STATISTIC.put(kind, (SystemConstant.MONITOR_STATISTIC.get(kind)+1));
            }
            if (SystemConstant.KIND_METADATA.equals(kind)) {
                MessageService.parseMetadata(line, fileName);
            } else if (SystemConstant.KIND_EVENT.equals(kind)) {
                EventTrans.do_trans(line);
            } else if (SystemConstant.KIND_ASSET.equals(kind)) {
                AssetTrans.do_trans(line);
            }
        }
        fileInputStream.close();
        inputStreamReader.close();
        reader.close();
        FileUtil.delDir(file.getParent());
    }
    
    private long getId() {
        return Thread.currentThread().getId();
    }
    
}
