
package soc.storm.situation.test;

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;

// import org.apache.commons.codec.digest.DigestUtils;

public class CommonTest {

    public static void main(String[] args) {
        md5Test();
    }

    public static void md5Test() {
        System.out.println(DigestUtils.md5Hex("name + password").toLowerCase());
    }

    // 32d9bbc19d986eebc61a63253d5ddc94
}
