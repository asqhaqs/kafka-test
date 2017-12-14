
package soc.storm.situation.test.encrypt;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * 
 * @author wangbin03
 *
 */
public class AESUtil {
    private static final String AES = "AES";
    private static final String CHARSET_NAME = "utf-8";

    private final static String KEYAES = "rNnbT+XwtYpBaP9ec6tRAQ==";

    // private final static String KEYAES = "aaa";

    /**
     * 获取密钥
     * 
     * @param password
     *            加密密码
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static SecretKeySpec getKey(String password) throws NoSuchAlgorithmException {
        // 密钥加密器生成器
        KeyGenerator kgen = KeyGenerator.getInstance(AES);
        kgen.init(128, new SecureRandom(password.getBytes()));

        // 创建加密器
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();

        SecretKeySpec key = new SecretKeySpec(enCodeFormat, AES);

        return key;
    }

    /**
     * 加密
     * 
     * @param str 原文
     * @param password 加密密码
     * @return
     */
    public static String encode(String str, String password) {
        byte[] arr = encodeToArr(str, password);
        // String strs = new Base64Encoder().encode(arr).replaceAll("[\\s*\t\n\r]", "");
        String strs = Base64.encodeBase64String(arr).replaceAll("[\\s*\t\n\r]", "");
        return strs;

        // return byteArrToString(arr);
    }

    /**
     * 
     * 加密
     * 
     * @author wuhongbo
     * @param str 原文
     * @param password 加密密码
     * @return
     */
    private static byte[] encodeToArr(String str, String password) {
        try {
            Cipher cipher = Cipher.getInstance(AES);// 创建密码器
            byte[] byteContent = str.getBytes(CHARSET_NAME);

            cipher.init(Cipher.ENCRYPT_MODE, getKey(password));// 初始化
            byte[] result = cipher.doFinal(byteContent);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 解密
     * 
     * @param hexStr 密文
     * @param password 加密密码
     * @return
     */
    public static String decode(String hexStr, String password) {
        if (hexStr == null) {
            return null;
        }

        try {
            // byte[] arr = string2ByteArr(hexStr);
            // byte[] arr = new BASE64Decoder().decodeBuffer(hexStr);
            byte[] arr = Base64.decodeBase64(hexStr);
            return decode(arr, password);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 解密
     * 
     * @param arr 密文数组
     * @param password 加密密码
     * @return
     */
    private static String decode(byte[] arr, String password) {
        try {
            // 创建密码器
            Cipher cipher = Cipher.getInstance(AES);
            cipher.init(Cipher.DECRYPT_MODE, getKey(password));// 初始化

            byte[] result = cipher.doFinal(arr);
            return new String(result, CHARSET_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 加密 add zhongsanmu 20160831
     * 
     * @param data
     * @return
     * @throws Exception
     */
    public static String encrypt(String data) {
        // // delete zhongsanmu 20170217
        // byte[] bt = encrypt(data.getBytes(), KEY.getBytes());
        // String strs = new BASE64Encoder().encode(bt).replaceAll("[\\s*\t\n\r]", "");
        // return strs;
        //
        // // return AESUtil.encrypt(data);

        return encode(data, KEYAES);

    }

    /**
     * 解密 add zhongsanmu 20160831
     * 
     * @param data
     * @return
     * @throws Exception
     */
    public static String decrypt(String data) {
        if (data == null) {
            return null;
        }

        return decode(data, KEYAES);
    }

    public static void main(String[] args) {
        // String password = "wuhongbo";
        // String content = "20130809,17:30,会员,";
        //
        // System.out.println("原文：" + content);
        //
        // String hexStr = encode(content, password);
        // System.out.println("密文长度：" + hexStr.length());
        // System.out.println("密文：" + hexStr);
        // System.out.println("解密：" + decode(hexStr, password));

        // String loginToken = AESUtil.encrypt("11ddd211ddd211ddd211ddd211ddd211ddd211ddd211ddd211ddd2");//
        String loginToken = AESUtil
                .encrypt("type:3accesstime:2014-11-27 09:35:59.045753sip:10.18.67.28sport:51001dip:192.168.0.223dport:53type:requesthost:quote");//
        System.out.println("loginToken:" + loginToken);// loginToken:SL4OO2Afi7s=
        String loginTokenReal = AESUtil.decrypt(loginToken);//
        System.out.println("loginTokenReal:" + loginTokenReal);// loginTokenReal:112
    }

}
