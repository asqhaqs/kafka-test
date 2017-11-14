
package soc.storm.situation.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;

/**
 * 
 * @author wangbin03
 *
 */
public class LzoCompress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static byte[] compress(byte[] sourceByteArray) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(4096);

            //
            LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO1X;
            LzoCompressor lzoCompressor = LzoLibrary.getInstance().newCompressor(lzoAlgorithm, null);
            LzoOutputStream lzoOutputStream = new LzoOutputStream(byteArrayOutputStream, lzoCompressor, 256);
            lzoOutputStream.write(sourceByteArray);
            lzoOutputStream.close();

            return byteArrayOutputStream.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
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
    public static byte[] deCommpress(byte[] compressByteArray) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressByteArray);

            //
            LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO1X;
            LzoDecompressor lzoDecompressor = LzoLibrary.getInstance().newDecompressor(lzoAlgorithm, null);
            LzoInputStream lzoInputStream = new LzoInputStream(byteArrayInputStream, lzoDecompressor);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = lzoInputStream.read(bytes)) != -1) {
                byteArrayOutputStream.write(bytes, 0, read);
            }
            byteArrayOutputStream.close();
            lzoInputStream.close();

            return byteArrayOutputStream.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
