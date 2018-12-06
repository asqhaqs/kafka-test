package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.support.service.MessageService;
import cn.situation.data.EventTrans;
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
    private MessageService messageService = new MessageService();

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
                handleEvent(filePath, fileName);
            }
            if (SystemConstant.KIND_METADATA.equals(kind)) {
                handleMetadata(type, filePath, fileName);
            }
            if (SystemConstant.KIND_ASSERT.equals(kind)) {
                //TODO 资产处理逻辑
            }
            //sqliteUtil.executeUpdate(sqliteUtil.getUpdateSql(kind, type, fileName));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void handleMetadata(String type, String remotePath, String fileName) {
        try {
            boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                    type, SystemConstant.PACKAGE_SUFFIX, false);
            LOG.info(String.format("[%s]: type<%s>, remotePath<%s>, fileName<%s>, result<%s>", "handleMetadata",
                    type, remotePath, fileName, result));
            if (result) {
                List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
                for (File file : fileList) {
                    List<String> lines = FileUtil.getFileContentByLine(file.getAbsolutePath(), true);
                    if (null != lines && !lines.isEmpty()) {
                        for (String line : lines) {
                            messageService.parseMetadata(line);
                        }
                    } else {
                        LOG.error(String.format("[%s]: lines<%s>, fileName<%s>, message<%s>", "handleMetadata",
                                lines, file.getName(), "文件内容为空."));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void handleEvent(String remotePath, String fileName) {
        try {
            boolean result = sftpUtil.downLoadOneFile(remotePath, fileName, SystemConstant.LOCAL_FILE_DIR,
                    SystemConstant.EVENT_PREFIX, SystemConstant.PACKAGE_SUFFIX, false);
            LOG.info(String.format("[%s]: remotePath<%s>, fileName<%s>, result<%s>",
                    "handleEvent", remotePath, fileName, result));
            if (result) {
                List<File> fileList = FileUtil.unTarGzWrapper(fileName, true);
                for (File file : fileList) {
                    List<String> eventList = FileUtil.getFileContentByLine(file.getAbsolutePath(), true);
                    LOG.info(String.format("[%s]: eventList<%s>",
                            "handleEvent", eventList));
                    EventTrans.do_trans(eventList);
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
