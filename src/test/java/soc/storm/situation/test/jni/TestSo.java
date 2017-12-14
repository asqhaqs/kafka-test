
package soc.storm.situation.test.jni;

import com.sun.jna.Library;

/**
 * 
 * @author wangbin03
 *
 */
public class TestSo {

    public static LgetLib INSTANCE = null;// = (LgetLib) Native.loadLibrary("aes", LgetLib.class);
    static {
        // System.load("libaes.so");
        // 调用linux下面的so文件,注意，这里只要写test就可以了，不要写libtest，也不要加后缀
        // INSTANCE = (LgetLib) Native.loadLibrary("aes", LgetLib.class);
    }

    /**
     * 
     * @author wangbin03
     *
     */
    public interface LgetLib extends Library {
        int init_aes(String file);

        int encrypt(String data_buff, int data_len, char[] encrypt_buff);

        int decrypt(String data_buff, int data_len, char[] decrypt_buff);
    }

    public int init_aes(String file) {
        return INSTANCE.init_aes(file);
    }

    public int encrypt(String data_buff, int data_len, char[] encrypt_buff) {
        return INSTANCE.encrypt(data_buff, data_len, encrypt_buff);
    }

    public int decrypt(String data_buff, int data_len, char[] decrypt_buff) {
        return INSTANCE.decrypt(data_buff, data_len, decrypt_buff);
    }

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        TestSo ts = new TestSo();

        String test_data_buff = "type:3accesstime:2014-11-27 09:35:59.045753sip:10.18.67.28sport:51001dip:192.168.0.223dport:53type:requesthost:quote";
        char[] encry_buff = null;
        char[] decrypt_buff = null;
        int len = test_data_buff.length();
        ts.init_aes("decrypt.conf");
        System.out.println("nraw : " + test_data_buff + "\n");
        len = ts.encrypt(test_data_buff, len, encry_buff);
        System.out.println("encrypt len : " + len + "\n");
        int len2 = ts.decrypt(String.valueOf(encry_buff), len, decrypt_buff);
        System.out.println("src_len: " + len2 + "\n");
        System.out.println("out : " + decrypt_buff + "\n");
    }
}
