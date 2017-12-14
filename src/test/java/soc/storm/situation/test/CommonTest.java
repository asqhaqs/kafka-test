
package soc.storm.situation.test;

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;

import soc.storm.situation.utils.DateTimeUtils;

// import org.apache.commons.codec.digest.DigestUtils;

public class CommonTest {

    public static void main(String[] args) {
        // md5Test();
        // dateTimeFormat();
        strToByteArray();
    }

    public static void md5Test() {
        System.out.println(DigestUtils.md5Hex("name + password").toLowerCase());
        // 32d9bbc19d986eebc61a63253d5ddc94
    }

    public static void dateTimeFormat() {
        System.out.println("dateString:" + DateTimeUtils.formatNowTime());
    }

    public static void strToByteArray() {
        String dataSourceStr = "aabbcc";
        byte[] dataSourceStrByteArray = dataSourceStr.getBytes();
        for (byte b : dataSourceStrByteArray) {
            System.out.print((char) b);
        }
        System.out.println();
    }

}
