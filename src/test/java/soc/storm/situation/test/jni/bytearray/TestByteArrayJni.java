
package soc.storm.situation.test.jni.bytearray;

public class TestByteArrayJni {

    private native byte[] encrypt(byte[] data_buff, int data_len, int encrypt_type, String encrypt_key);

    private native byte[] decrypt(byte[] data_buff, int data_len, int encrypt_type, String key);
}
