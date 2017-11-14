
package soc.storm.situation.compress;

import java.io.IOException;

import org.xerial.snappy.Snappy;

/**
 * 
 * @author wangbin03
 *
 */
public class SnappyCompress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static byte[] compress(byte[] sourceByteArray) {
        try {
            return Snappy.compress(sourceByteArray);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * deCompress
     * 
     * @param compressByteArray
     * @return
     */
    public static byte[] deCommpress(byte[] compressByteArray) {
        try {
            return Snappy.uncompress(compressByteArray);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
