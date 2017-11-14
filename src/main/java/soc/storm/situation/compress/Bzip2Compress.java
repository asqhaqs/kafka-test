
package soc.storm.situation.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 * 
 * @author wangbin03
 *
 */
public class Bzip2Compress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static byte[] compress(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
            BZip2CompressorOutputStream bcos = new BZip2CompressorOutputStream(baos);
            bcos.write(data);
            bcos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * deCompress
     * 
     * @param compressByteArray
     * @return
     */
    public static byte[] deCommpress(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
            BZip2CompressorInputStream bcis = new BZip2CompressorInputStream(new ByteArrayInputStream(data));
            int count;
            byte[] buffer = new byte[8192];
            while ((count = bcis.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            bcis.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
