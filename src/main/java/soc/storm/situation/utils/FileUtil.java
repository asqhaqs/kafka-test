
package soc.storm.situation.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class FileUtil {

    public static void readFileContent(String filename) throws Exception {
        File f = new File(filename);
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println("--------------------------------->>>>>" + line);
        }
        reader.close();
    }

    public static void testConfigFile(String appModule) throws Exception {
        // String aa = SystemConstants.BROKER_URL;
        System.out.println("--------------------------------:" + appModule + "--" + System.getProperty("java.security.auth.login.config"));
        System.out.println("--------------------------------:" + appModule + "--" + System.getProperty("java.security.krb5.conf"));
        FileUtil.readFileContent(System.getProperty("java.security.auth.login.config"));
        FileUtil.readFileContent(System.getProperty("java.security.krb5.conf"));
    }
}
