package cn.situation.util;

import org.slf4j.Logger;

import java.security.MessageDigest;

/**
 * @author lenzhao
 * @date 2018/12/10 16:18
 * @description TODO
 */
public class MD5Util {

    private static final Logger LOG = LogUtil.getInstance(MD5Util.class);

    public static char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
    /**
     * 获取32位加密串
     * @param inputStr
     * @return
     */
    public final static String md5Encode(String inputStr) {
        String encode="";
        try {
            byte[] btInput = inputStr.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            encode=new String(str);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return encode;
    }
    /**
     * 对字符串进行MD5加密
     * @param inputStr
     * @return
     */
    public static String encodeToStr(String inputStr){
        MD5 md5 = new MD5();
        return  md5.getMD5ofStr(inputStr);
    }
}
