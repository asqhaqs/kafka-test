package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.util.FileUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.SFTPUtil;
import cn.situation.util.SqliteUtil;
import net.sf.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.zeromq.ZMQ;

public class Worker implements Runnable {

    private static final Logger LOG = LogUtil.getInstance(Worker.class);

    private static SqliteUtil sqliteUtil = SqliteUtil.getInstance();

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

    private void action(byte[] data) throws Exception {
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
            }
            if (SystemConstant.KIND_ASSERT.equals(kind)) {
                //TODO 资产处理逻辑
            }
            String sql = "UPDATE t_position SET file_name='" + fileName + "' WHERE kind='" + kind + "' AND type='" + type + "'";
            sqliteUtil.executeUpdate(sql);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }
    
    private void alertEvent(String remotePath, String fileName, String localPath) {
    	SFTPUtil sftp = new SFTPUtil();
    	String filename = sftp.downLoadOneFile(remotePath, fileName, localPath, "", ".gz", true);
    	File file = new File(filename);
    	try {
    		String outputDir = remotePath + "\\" + fileName.split(".")[0];
    		List<File> fileList = FileUtil.unTarGz(file, outputDir);
			if(fileList != null && fileList.size() > 0) {
				File eventFile = fileList.get(0);
				List<String> eventList = FileUtil.getFileContentByLine(eventFile.getPath());
//				System.out.println(eventList.toString());
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private long getId() {
        return Thread.currentThread().getId();
    }
}
