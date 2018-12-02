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

    public static List<String> getFileContentByLine(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return null;
        }
        List<String> content = new ArrayList<String>();
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String lineContent = "";
            while ((lineContent = reader.readLine()) != null) {
                content.add(lineContent);
                if (lineContent.contains("$"))
                    System.out.println(lineContent);
            }

            fileInputStream.close();
            inputStreamReader.close();
            reader.close();
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
        File file=new File(path + "/" + fileName);
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
}
