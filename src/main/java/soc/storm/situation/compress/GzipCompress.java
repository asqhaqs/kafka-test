
package soc.storm.situation.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 
 * @author wangbin03
 *
 */
public class GzipCompress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static final byte[] compress(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(data);
            gzip.finish();
            return out.toByteArray();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                gzip.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * deCompress
     * 
     * @param compressByteArray
     * @return
     */
    public static final byte[] deCommpress(byte[] bytes) {
        try {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes));
            int count;
            byte[] buffer = new byte[8192];
            while ((count = gis.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            gis.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
