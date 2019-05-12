package cn.situation.util;

import org.slf4j.Logger;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

    private static final Logger LOG = LogUtil.getInstance(FileUtil.class);

    public static String getFileContent(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return null;
        }

        StringBuffer content = new StringBuffer();
        try {
            char[] temp = new char[1024];
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            while (inputStreamReader.read(temp) != -1) {
                content.append(new String(temp));
                temp = new char[1024];
            }

            fileInputStream.close();
            inputStreamReader.close();
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
        }

        return content.toString();
    }

    public static List<String> getFileContentByLine(String filePath, boolean ifDelFileDir) throws Exception {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile() || file.length() == 0) {
            return null;
        }
        List<String> content = new ArrayList<>();
        FileInputStream fileInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        BufferedReader reader = new BufferedReader(inputStreamReader, 5242880);
        String lineContent;
        while ((lineContent = reader.readLine()) != null) {
            content.add(lineContent);
        }
        fileInputStream.close();
        inputStreamReader.close();
        reader.close();
        if (ifDelFileDir) {
            delDir(file.getParent());
        }
        return content;
    }

    public static void writeFile(String path, String fileName, String content) throws IOException {
        FileOutputStream fos = new FileOutputStream(path + "/" + fileName);
        BufferedOutputStream buff = new BufferedOutputStream(fos);
        buff.write(content.getBytes("UTF-8"));
        buff.flush();
        buff.close();
        fos.close();
    }

    public static void writeFile(String path, String fileName, List<Object> dataList, boolean append) {
        try {
            if (null == dataList || dataList.isEmpty()) {
                return;
            }
            FileOutputStream fos = new FileOutputStream(path + File.separator + fileName, append);
            BufferedOutputStream buff = new BufferedOutputStream(fos);
            for (Object data : dataList) {
                buff.write((byte[]) data);
                buff.write(System.getProperty("line.separator").getBytes());
            }
            buff.flush();
            buff.close();
            fos.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void createDir(String filePath) {
        File dir = new File(filePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public static void delFile(String path, String fileName) {
        String filePath = path.endsWith("/") ? (path + fileName) : (path + "/" + fileName);
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            LOG.debug(String.format("[%s]: path<%s>, fileName<%s>", "delFile", path, fileName));
            file.delete();
        }
    }

    public static void delDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] tmp = dir.listFiles();
            for (int i=0; i<tmp.length; i++) {
                if (tmp[i].isDirectory()) {
                    delDir(path + "/" + tmp[i].getName());
                } else {
                    tmp[i].delete();
                }
            }
            dir.delete();
        }
    }

    public static void reNameFile(String path, String oldName, String newName) {
        if (!oldName.equals(newName)) {
            File oldFile = new File(path + "/" + oldName);
            File newFile = new File(path + "/" + newName);
            if(!newFile.exists()) {
                oldFile.renameTo(newFile);
            }
        }
    }
    
    /** 
	     * 构建目录 
	     * @param outputDir 
	     * @param subDir 
	     */  
    private static void createDirectory(String outputDir,String subDir) {
        File file = new File(outputDir);  
        if(!(subDir == null || subDir.trim().equals(""))){//子目录不为空  
            file = new File(outputDir + "/" + subDir);  
        }  
        if(!file.exists()){  
              if(!file.getParentFile().exists())
                  file.getParentFile().mkdirs();
            file.mkdirs();  
        }  
    }

    /**
     * 根据压缩包文件名获取消费位置信息
     * @param fileName
     * @return
     */
    public static long getPositionByFileName(String fileName) {
        return Long.parseLong(fileName.substring(fileName.lastIndexOf("_") + 1, fileName.indexOf(".")));
    }
    
    public static void main(String[] args) {

	}
}
