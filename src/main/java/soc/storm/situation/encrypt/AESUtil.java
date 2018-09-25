
package soc.storm.situation.encrypt;

import soc.storm.situation.contants.SystemConstants;

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * 
 * @author wangbin03
 *
 */
public class AESUtil {

    public static LgetLib INSTANCE = null;// = (LgetLib) Native.loadLibrary("aes", LgetLib.class);
    static {
        // System.load("libaes.so");
        // 调用linux下面的so文件,注意，这里只要写test就可以了，不要写libtest，也不要加后缀
        INSTANCE = (LgetLib) Native.loadLibrary("aes", LgetLib.class);
    }

    /**
     * 
     * @author wangbin03
     *
     */
    public interface LgetLib extends Library {
        int init_aes(String file);

        int encrypt(byte[] data_buff, int data_len, byte[] encrypt_buff);

        int decrypt(byte[] data_buff, int data_len, byte[] decrypt_buff);
    }

    public int init_aes(String file) {
        return INSTANCE.init_aes(file);
    }

    public int encrypt(byte[] data_buff, int data_len, byte[] encrypt_buff) {
        return INSTANCE.encrypt(data_buff, data_len, encrypt_buff);
    }

    public int decrypt(byte[] data_buff, int data_len, byte[] decrypt_buff) {
        return INSTANCE.decrypt(data_buff, data_len, decrypt_buff);
    }

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        AESUtil ts = new AESUtil();

        // String test_data_buff =
        // "type:3accesstime:2014-11-27 09:35:59.045753sip:10.18.67.28sport:51001dip:192.168.0.223dport:53type:requesthost:quote";

        String dataSourceStr = "aabbcc";
        byte[] dataSourceStrByteArray = dataSourceStr.getBytes();

        byte[] encry_buff = new byte[1000];
        byte[] decrypt_buff = new byte[1000];
        // int len = test_data_buff.length();
        int len = dataSourceStr.length();
        int initResult = ts.init_aes(SystemConstants.FILE_PATH + "/decrypt.conf");
        // int initResult = ts.init_aes("/decrypt.conf");
        // int initResult = ts.init_aes("/home/storm/geoipdata/decrypt.conf");
        System.out.println("------initResult:" + initResult);

        // System.out.println("nraw : " + test_data_buff + "\n");
        System.out.println("nraw : " + dataSourceStr + "\n");

        // len = ts.encrypt(test_data_buff.getBytes(), len, encry_buff);
        len = ts.encrypt(dataSourceStrByteArray, len, encry_buff);
        System.out.println("encrypt len : " + len + ", encry_buff:" + byteArrayToString(encry_buff) + "\n");

        int len2 = ts.decrypt(encry_buff, len, decrypt_buff);
        System.out.println("src_len: " + len2 + "\n");
        System.out.println("out : " + byteArrayToString(decrypt_buff).toString() + "\n");

        //
        byte[] destBytes = subBytes(decrypt_buff, 0, len2);
        System.out.println("destBytes : " + byteArrayToString(destBytes).toString() + "\n");
    }

    public static String byteArrayToString(byte[] byteArray) {
        System.out.println("byteArrayToString----------byteArray.length:" + byteArray.length);
        StringBuilder stringBuilder = new StringBuilder();
        for (byte b : byteArray) {
            stringBuilder.append((char) b);
        }
        return stringBuilder.toString();
    }

    public static byte[] subBytes(byte[] src, int begin, int count) {
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }
}
