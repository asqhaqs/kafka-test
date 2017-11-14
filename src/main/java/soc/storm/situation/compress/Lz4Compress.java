
package soc.storm.situation.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * 
 * @author wangbin03
 *
 */
public class Lz4Compress {

    /**
     * compress
     * 
     * @param sourceByteArray
     * @return
     */
    public static byte[] compress(byte[] sourceByteArray) {
        try {
            LZ4Factory factory = LZ4Factory.fastestInstance();
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            LZ4Compressor compressor = factory.fastCompressor();
            LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, 8192, compressor);
            compressedOutput.write(sourceByteArray);
            compressedOutput.close();
            return byteOutput.toByteArray();
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
            LZ4Factory factory = LZ4Factory.fastestInstance();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
            LZ4FastDecompressor decompresser = factory.fastDecompressor();
            LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(compressByteArray), decompresser);
            int count;
            byte[] buffer = new byte[8192];
            while ((count = lzis.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            lzis.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
