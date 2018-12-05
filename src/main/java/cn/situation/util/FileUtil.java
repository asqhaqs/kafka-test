package cn.situation.util;

import cn.situation.cons.SystemConstant;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.slf4j.Logger;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

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

    public static List<String> getFileContentByLine(String filePath, boolean ifDelFileDir) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile() || file.length() == 0) {
            return null;
        }
        List<String> content = new ArrayList<String>();
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String lineContent;
            while ((lineContent = reader.readLine()) != null) {
                content.add(lineContent);
                if (lineContent.contains("$"))
                    System.out.println(lineContent);
            }
            fileInputStream.close();
            inputStreamReader.close();
            reader.close();
            if (ifDelFileDir) {
                delDir(file.getParent());
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
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
	     * 解压tar.gz 文件 
	     * @param oriFilePath 要解压的tar.gz文件路径
	     * @param outputDir 要解压到某个指定的目录下 
	     * @throws IOException 
	     */  
    public static List<File> unTarGz(String oriFilePath, String outputDir) throws IOException {
        List<File> fileList = new ArrayList<>();
        TarInputStream tarIn = new TarInputStream(new GZIPInputStream(
                new BufferedInputStream(new FileInputStream(oriFilePath))),
                1024 * 2);
        createDirectory(outputDir,null); //创建输出目录
        TarEntry entry;
        while ((entry = tarIn.getNextEntry()) != null) {
            if (entry.isDirectory()) {//是目录
                entry.getName();
                createDirectory(outputDir, entry.getName());//创建空目录
            } else { //是文件
                File tmpFile = new File(outputDir + "/" + entry.getName());
                OutputStream out = new FileOutputStream(tmpFile);
                int length;
                byte[] b = new byte[2048];
                while ((length = tarIn.read(b)) != -1) {
                    out.write(b, 0, length);
                }
                fileList.add(tmpFile);
                out.close();
            }
        }
        tarIn.close();
        return fileList;
    }

    /**
     * unTarGz包装器
     * @param fileName
     * @return
     * @throws IOException
     */
    public static List<File> unTarGzWrapper(String fileName, boolean ifDelOriFile) throws IOException {
        String outputDir = SystemConstant.LOCAL_FILE_DIR + fileName.substring(0, fileName.indexOf("."));
        String oriFilePath =  SystemConstant.LOCAL_FILE_DIR + fileName;
        List<File> fileList = unTarGz(oriFilePath, outputDir);
        if (ifDelOriFile) {
            delFile(SystemConstant.LOCAL_FILE_DIR, fileName);
        }
        return fileList;
    }
    
    /** 
	     * 构建目录 
	     * @param outputDir 
	     * @param subDir 
	     */  
    public static void createDirectory(String outputDir,String subDir){     
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
    public static int getPositionByFileName(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.lastIndexOf("_") + 1, fileName.indexOf(".")));
    }
    
    public static void main(String[] args) {

	}
}
