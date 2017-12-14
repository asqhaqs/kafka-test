
package soc.storm.situation.test.encrypt;

import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * MD5工具类，加盐
 * 
 * @author zhongsanmu
 * @time 20170216
 */
public class MD5Util {

    /**
     * 获取十六进制字符串形式的MD5摘要
     */
    private static String md5Hex(String src) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bs = md5.digest(src.getBytes());
            return new String(new Hex().encode(bs));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 加盐MD5
     * 
     * @author zhongsanmu
     * @time 20170216
     * @param password
     * @return
     * 
     *         notice:
     *         后期：将“随机数salt”和“DES的秘钥key”存到另外的数据库，并且与主数据分机部署。
     */
    public static String generate(String password) {
        String salt = RandomStringUtils.random(16, true, true).toLowerCase();
        password = md5Hex(password + salt);

        // System.out.println("-----generate-----salt:" + salt);
        // System.out.println("-----generate-----password:" + password);

        return password + salt;
    }

    /**
     * 校验加盐后是否和原文一致
     * 
     * @author zhongsanmu
     * @time 20170216
     * @param password
     * @param md5
     * @return
     */
    public static boolean verify(String password, String md5) {
        String salt = md5.substring(32);
        String passwordCrpt = md5.substring(0, 32);

        // System.out.println("-----verify-----salt:" + salt);
        // System.out.println("-----verify-----passwordCrpt:" + passwordCrpt);

        return md5Hex(password + salt).equals(new String(passwordCrpt));
    }

    // 测试主函数
    public static void main(String args[]) {
        String plaintext = "123456";
        String ciphertext = MD5Util.generate(plaintext);
        System.out.println("加盐后MD5：" + ciphertext);
        System.out.println("是否是同一字符串:" + MD5Util.verify(plaintext, ciphertext));
    }

}
