package cn.situation.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import cn.situation.cons.SystemConstant;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.slf4j.Logger;

/**
 * @author lenzhao
 * @date 2018/12/2 14:15
 * @description TODO
 */
public class SFTPUtil {

    private static Logger LOG = LogUtil.getInstance(SFTPUtil.class);

    private String host;
    private String username;
    private String password;
    private int port = 22;
    private ChannelSftp sftp = null;
    private Session sshSession = null;
    private Channel channel;

    public SFTPUtil() {
        this.host = SystemConstant.SFTP_HOST;
        this.username = SystemConstant.SFTP_USERNAME;
        this.password = SystemConstant.SFTP_PASSWORD;
        if (!StringUtil.isBlank(SystemConstant.SFTP_PORT)) {
            this.port = Integer.parseInt(SystemConstant.SFTP_PORT);
        }
    }

    public SFTPUtil(String host, int port, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.port = port;
    }

    public SFTPUtil(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
    }

    /**
     * 通过SFTP连接服务器
     */
    private void connect() {
        try {
            JSch jsch = new JSch();
            jsch.getSession(username, host, port);
            sshSession = jsch.getSession(username, host, port);
            LOG.debug(String.format("[%s]: message<%s>", "connect", "Session created."));
            sshSession.setPassword(password);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            sshSession.setConfig(sshConfig);
            sshSession.connect();
            LOG.debug(String.format("[%s]: message<%s>", "connect", "Session connected."));
            channel = sshSession.openChannel("sftp");
            channel.connect();
            LOG.debug(String.format("[%s]: message<%s>", "connect", "Opening Channel."));
            sftp = (ChannelSftp) channel;
            LOG.debug(String.format("[%s]: message<%s>", "connect", "Connected to " + host + "."));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 关闭连接
     */
    private void disconnect() {
        if (null != sftp) {
            sftp.quit();
        }
        if (null != channel && channel.isConnected()) {
            channel.disconnect();
        }
        if (null != sshSession && sshSession.isConnected()) {
            sshSession.disconnect();
        }
        LOG.debug(String.format("[%s]: message<%s>", "disconnect", "session closed."));
    }

    public List<String> getRemoteFileName(String remotePath, String fileFormat, String fileEndFormat, int position) {
        LOG.info(String.format("[%s]: remotePath<%s>, fileFormat<%s>, fileEndFormat<%s>, position<%s>",
                "getRemoteFileName", remotePath, fileFormat, fileEndFormat, position));
        List<String> fileNameList = new ArrayList<>();
        try {
            connect();
            Vector v = listFiles(remotePath);
            if (v.size() > 0) {
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    LsEntry entry = (LsEntry) it.next();
                    String fileName = entry.getFilename();
                    SftpATTRS attrs = entry.getAttrs();
                    if (!attrs.isDir() && DateUtil.getSftpFileMtime(attrs.getMtimeString()) >=
                            Integer.parseInt(SystemConstant.SFTP_FILE_NO_CHANGE_INTERVAL)) {
                        if (checkFileName(fileName, fileFormat, fileEndFormat) && filterFileName(fileName, position)) {
                            fileNameList.add(fileName);
                        }
                    }
                }
            }
            sortFileName(fileNameList);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            disconnect();
        }
        return fileNameList;
    }

    public Map<String, Long> getRemoteFileName(String remotePath, String fileFormat, String fileEndFormat, int position,
                                               String kind, String type) {
        LOG.info(String.format("[%s]: remotePath<%s>, fileFormat<%s>, fileEndFormat<%s>, position<%s>, kind<%s>, type<%s>",
                "getRemoteFileName", remotePath, fileFormat, fileEndFormat, position, kind, type));
        Map<String, Long> fileNameMap = new HashMap<>();
        try {
            connect();
            Vector v = listFiles(remotePath);
            if (v.size() > 0) {
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    LsEntry entry = (LsEntry) it.next();
                    String fileName = entry.getFilename();
                    SftpATTRS attrs = entry.getAttrs();
                    if (!attrs.isDir() && DateUtil.getSftpFileMtime(attrs.getMtimeString()) >=
                            Integer.parseInt(SystemConstant.SFTP_FILE_NO_CHANGE_INTERVAL)) {
                        if (checkFileName(fileName, fileFormat, fileEndFormat) && filterFileName(fileName, position)) {
                            fileNameMap.put(JsonUtil.pack2Json(remotePath, fileName, kind, type),
                                    DateUtil.getSftpFileMtime(attrs.getMtimeString()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            disconnect();
        }
        return fileNameMap;
    }

    /**
     * 检查文件名称
     * @param fileName
     * @param fileFormat
     * @param fileEndFormat
     * @return
     */
    private boolean checkFileName(String fileName, String fileFormat, String fileEndFormat) {
        boolean result = false;
        if (!StringUtil.isBlank(fileName)) {
            if (!StringUtil.isBlank(fileFormat) && !StringUtil.isBlank(fileEndFormat)) {
                result = fileName.startsWith(fileFormat) && fileName.endsWith(fileEndFormat);
            } else if (!StringUtil.isBlank(fileFormat) && StringUtil.isBlank(fileEndFormat)) {
                result = fileName.startsWith(fileFormat);
            } else if (StringUtil.isBlank(fileFormat) && !StringUtil.isBlank(fileEndFormat)) {
                result = fileName.endsWith(fileEndFormat);
            } else if (StringUtil.isBlank(fileFormat) && StringUtil.isBlank(fileEndFormat)) {
                result = true;
            }
        }
        return result;
    }

    /**
     * 根据文件名进行排序
     * @param list
     */
    private void sortFileName(List<String> list) {
        if (list.size() > 0) {
            list.sort((a, b) -> {
                int num1 = FileUtil.getPositionByFileName(a);
                int num2 = FileUtil.getPositionByFileName(b);
                if (num1 == num2)
                    return 0;
                if (num1 > num2)
                    return 1;
                return -1;
            });
        }
    }

    /**
     * 过滤文件名称
     * @param fileName
     * @param position
     * @return
     */
    private boolean filterFileName(String fileName, int position) {
        int num = FileUtil.getPositionByFileName(fileName);
        return num > position;
    }

    /**
     * 批量下载文件
     * @param remotePath: 远程下载目录(以路径符号结束,可以为相对路径eg:/assess/sftp/jiesuan_2/2014/)
     * @param localPath: 本地保存目录(以路径符号结束,D:\Duansha\sftp\)
     * @param fileFormat: 下载文件格式(以特定字符开头,为空不做检验)
     * @param fileEndFormat: 下载文件格式(文件格式)
     * @param del: 下载后是否删除sftp文件
     * @return
     */
    public List<String> batchDownLoadFile(String remotePath, String localPath,
                                          String fileFormat, String fileEndFormat, boolean del) {
        LOG.info(String.format("[%s]: remotePath<%s>, localPath<%s>, fileFormat<%s>, fileEndFormat<%s>, del<%s>",
                "batchDownLoadFile", remotePath, localPath, fileFormat, fileEndFormat, del));
        List<String> filenames = new ArrayList<>();
        if (!remotePath.endsWith("/")) {
            remotePath += "/";
        }
        try {
            File localDir = new File(localPath);
            if (!localDir.exists()) {
                localDir.mkdirs();
            }
            connect();
            Vector v = listFiles(remotePath);
            if (v.size() > 0) {
                LOG.info(String.format("[%s]: fileSize<%s>", "batchDownLoadFile", v.size()));
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    LsEntry entry = (LsEntry) it.next();
                    String filename = entry.getFilename();
                    SftpATTRS attrs = entry.getAttrs();
                    if (!attrs.isDir()) {
                        boolean flag;
                        String localFileName = localPath + filename;
                        fileFormat = StringUtil.trim(fileFormat);
                        fileEndFormat = StringUtil.trim(fileEndFormat);
                        if (checkFileName(filename, fileFormat, fileEndFormat)) {
                            flag = downloadFile(remotePath, filename,localPath, filename);
                            if (flag) {
                                filenames.add(localFileName);
                                if (del) {
                                    deleteSFTP(remotePath, filename);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "batchDownLoadFile", e.getMessage()));
        } finally {
            disconnect();
        }
        return filenames;
    }

    /**
     * 下载单个文件
     * @param remotePath: 远程下载目录(以路径符号结束,可以为相对路径eg:/assess/sftp/jiesuan_2/2014/)
     * @param remoteFileName: 下载文件名
     * @param localPath: 本地保存目录(以路径符号结束,D:\Duansha\sftp\)
     * @param fileFormat: 下载文件格式(以特定字符开头,为空不做检验)
     * @param fileEndFormat: 下载文件格式(文件格式)
     * @param del: 下载后是否删除sftp文件
     * @return
     */
    public boolean downLoadOneFile(String remotePath, String remoteFileName, String localPath,
                                          String fileFormat, String fileEndFormat, boolean del) {
        LOG.info(String.format("[%s]: remotePath<%s>, remoteFileName<%s>, localPath<%s>, fileFormat<%s>, " +
                        "fileEndFormat<%s>, del<%s>", "downLoadOneFile", remotePath, remoteFileName, localPath,
                fileFormat, fileEndFormat, del));
        boolean flag = false;
        if (!remotePath.endsWith("/")) {
            remotePath += "/";
        }
        try {
            connect();
            if (checkFileName(remoteFileName, fileFormat, fileEndFormat)) {
                flag = downloadFile(remotePath, remoteFileName, localPath, remoteFileName);
                if (flag && del) {
                    deleteSFTP(remotePath, remoteFileName);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            disconnect();
        }
        return flag;
    }

    /**
     * 下载单个文件
     * @param remotePath: 远程下载目录(以路径符号结束)
     * @param remoteFileName: 下载文件名
     * @param localPath: 本地保存目录(以路径符号结束)
     * @param localFileName: 保存文件名
     * @return
     */
    public boolean downloadFile(String remotePath, String remoteFileName,String localPath, String localFileName)
            throws Exception {
        File file = new File(localPath + localFileName);
        try (FileOutputStream output = new FileOutputStream(file)) {
            sftp.get(remotePath + remoteFileName, output);
            LOG.debug(String.format("[%s]: remotePath<%s>, remoteFileName<%s>, localPath<%s>, localFileName<%s>, " +
                            "message<%s>", "downloadFile", remotePath, remoteFileName, localPath, localFileName,
                    "Download success from sftp."));
        }
        return true;
    }

    /**
     * 上传单个文件
     * @param remotePath: 远程保存目录
     * @param remoteFileName: 保存文件名
     * @param localPath: 本地上传目录(以路径符号结束)
     * @param localFileName: 上传的文件名
     * @return
     */
    public boolean uploadFile(String remotePath, String remoteFileName,String localPath, String localFileName) {
        FileInputStream in = null;
        try {
            createDir(remotePath);
            File file = new File(localPath + localFileName);
            in = new FileInputStream(file);
            sftp.put(in, remoteFileName);
            LOG.debug(String.format("[%s]: remoteFileName<%s>, message<%s>", "uploadFile", remoteFileName, "upload success."));
            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        return false;
    }

    /**
     * 批量上传文件
     * @param remotePath: 远程保存目录
     * @param localPath: 本地上传目录(以路径符号结束)
     * @param del: 上传后是否删除本地文件
     * @return
     */
    public boolean bacthUploadFile(String remotePath, String localPath, boolean del) {
        try {
            connect();
            File localDir = new File(localPath);
            File[] files = localDir.listFiles();
            if (null != files && files.length > 0) {
                LOG.info(String.format("[%s]: localPath<%s>, fileSize<%s>", "bacthUploadFile", localPath, files.length));
                for (File file : files) {
                    if (file.isFile() && !file.getName().contains("bak")) {
                        if (this.uploadFile(remotePath, file.getName(), localPath, file.getName()) && del) {
                            deleteFile(localPath + file.getName());
                        }
                    }
                }
            }
            return true;
        }
        catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            disconnect();
        }
        return false;

    }

    /**
     * 删除本地文件
     * @param filePath: 本地文件路径
     * @return
     */
    private boolean deleteFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return false;
        }
        if (!file.isFile()) {
            return false;
        }
        boolean rs = file.delete();
        LOG.debug(String.format("[%s]: filePath<%s>, message<%s>", "deleteFile", filePath, "delete file success from local."));
        return rs;
    }

    /**
     * 创建目录
     * @param createPath: 远程目录路径
     * @return
     */
    private boolean createDir(String createPath) {
        try
        {
            if (isDirExist(createPath)) {
                this.sftp.cd(createPath);
                return true;
            }
            String pathArry[] = createPath.split("/");
            StringBuilder filePath = new StringBuilder("/");
            for (String path : pathArry) {
                if (path.equals("")) {
                    continue;
                }
                filePath.append(path).append("/");
                if (isDirExist(filePath.toString())) {
                    sftp.cd(filePath.toString());
                } else {
                    sftp.mkdir(filePath.toString());
                    sftp.cd(filePath.toString());
                }
            }
            this.sftp.cd(createPath);
            return true;
        } catch (SftpException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * 判断目录是否存在
     * @param directory:
     * @return
     */
    private boolean isDirExist(String directory) {
        boolean isDirExistFlag = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(directory);
            isDirExistFlag = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isDirExistFlag = false;
            }
        }
        return isDirExistFlag;
    }

    /**
     * 删除stfp文件
     * @param directory：要删除文件所在目录
     * @param deleteFile：要删除的文件
     * @param
     */
    private void deleteSFTP(String directory, String deleteFile) {
        try {
            sftp.rm(directory + deleteFile);
            LOG.debug(String.format("[%s]: directory<%s>, deleteFile<%s>, message<%s>",
                    "deleteSFTP", directory, deleteFile, "delete file success from sftp."));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 如果目录不存在就创建目录
     * @param path:
     */
    public void mkdirs(String path) {
        File f = new File(path);
        String fs = f.getParent();
        f = new File(fs);
        if (!f.exists()) {
            f.mkdirs();
        }
    }

    /**
     * 列出目录下的文件
     * @param directory: 要列出的目录
     * @return
     */
    private Vector listFiles(String directory) throws SftpException {
        return sftp.ls(directory);
    }

    public static void main(String[] args) {

    }
}


