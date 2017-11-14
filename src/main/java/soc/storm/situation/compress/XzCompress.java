
package soc.storm.situation.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

/**
 * 
 * @author wangbin03
 *
 */
public class XzCompress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static byte[] compress(byte[] data) {
        try {
            LZMA2Options options = new LZMA2Options();
            // options.setMode(LZMA2Options.MODE_NORMAL);
            // options.setDepthLimit(9);
            // options.setLc(4);
            // options.setPb(0);
            // options.setPreset(8);
            // options.setNiceLen(64);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(8192);
            XZOutputStream out = new XZOutputStream(outputStream, options);
            out.write(data);
            out.finish();
            out.close();
            return outputStream.toByteArray();
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
            XZInputStream xzis = new XZInputStream(new ByteArrayInputStream(data));
            int count;
            byte[] buffer = new byte[8192];
            while ((count = xzis.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            xzis.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
