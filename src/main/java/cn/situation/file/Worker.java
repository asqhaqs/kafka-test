package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.util.FileUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.SFTPUtil;
import cn.situation.util.SqliteUtil;
import net.sf.json.JSONObject;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.zeromq.ZMQ;

public class Worker implements Runnable {

    private static final Logger LOG = LogUtil.getInstance(Worker.class);

    private static SqliteUtil sqliteUtil = SqliteUtil.getInstance();

    private SFTPUtil sftpUtil = new SFTPUtil();

    static {

    }

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
                LOG.error("[%s]: message<%s>, thread-id<%s>", "Worker.run", e.getMessage(), getId());
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXCEPT_INTERVAL_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        socket.close();
    }

    private void action(byte[] data) {
        try {
            JSONObject json = JSONObject.fromObject(new String(data));
            String kind = json.getString("kind");
            String type = json.getString("type");
            String filePath = json.getString("filePath");
            String fileName = json.getString("fileName");
            LOG.info(String.format("[%s]: kind<%s>, type<%s>, filePath<%s>, fileName<%s>", "action",
                    kind, type, filePath, fileName));
            if (SystemConstant.KIND_EVENT.equals(kind)) {
                //TODO 告警处理逻辑
            }
            if (SystemConstant.KIND_METADATA.equals(kind)) {
                //TODO 流量处理逻辑
                handleMetadata(type, filePath, fileName);
            }
            if (SystemConstant.KIND_ASSERT.equals(kind)) {
                //TODO 资产处理逻辑
            }
            sqliteUtil.executeUpdate(sqliteUtil.getUpdateSql(kind, type, fileName));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void handleMetadata(String type, String remotePath, String fileName) {
        try {
            boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                    type, SystemConstant.PACKAGE_SUFFIX, false);
            LOG.error(String.format("[%s]: type<%s>, remotePath<%s>, fileName<%s>, rsult<%s>", "handleMetadata",
                    type, remotePath, fileName, result));
            // TODO
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void alertEvent(String remotePath, String fileName) {
        try {
            boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                    SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
            LOG.error(String.format("[%s]: rsult<%s>", "alertEvent", result));
            if (result) {
                File file = new File(SystemConstant.LOCAL_FILE_DIR + fileName);
                String outputDir = remotePath + "\\" + fileName.substring(0, fileName.indexOf("."));
                List<File> fileList = FileUtil.unTarGz(file, outputDir);
                if(fileList != null && fileList.size() > 0) {
                    File eventFile = fileList.get(0);
                    List<String> eventList = FileUtil.getFileContentByLine(eventFile.getPath());
//				System.out.println(eventList.toString());
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
    
    private long getId() {
        return Thread.currentThread().getId();
    }
}
