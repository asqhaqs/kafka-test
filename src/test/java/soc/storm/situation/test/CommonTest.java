
package soc.storm.situation.test;

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;

import soc.storm.situation.utils.DateTimeUtils;

// import org.apache.commons.codec.digest.DigestUtils;

public class CommonTest {

    public static void main(String[] args) {
        // md5Test();
        dateTimeFormat();
    }

    public static void md5Test() {
        System.out.println(DigestUtils.md5Hex("name + password").toLowerCase());
        // 32d9bbc19d986eebc61a63253d5ddc94
    }

    public static void dateTimeFormat() {
        System.out.println("dateString:" + DateTimeUtils.formatNowTime());
    }

}
