
package soc.storm.situation.coder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author wangbin03
 *
 */
public class WebFlowLogGatherMsgBinCoder implements WebFlowLogGatherMsgCoder {

    /**
     * int-bytes
     * https://www.cnblogs.com/zzy-frisrtblog/p/5826511.html
     * 
     * @param i
     * @param buf
     * @param offset
     */
    public static void int2Bytes(int i, byte[] buf, int offset) {
        buf[offset] = (byte) i;
        i >>= 8;
        buf[offset + 1] = (byte) i;
        i >>= 8;
        buf[offset + 2] = (byte) i;
        i >>= 8;
        buf[offset + 3] = (byte) i;
    }

    /**
     * bytes-int
     * https://www.cnblogs.com/zzy-frisrtblog/p/5826511.html
     * 
     * @param bs
     * @return
     */
    private static int byte2Int(byte[] bs) {
        int retVal = 0;
        int len = bs.length < 4 ? bs.length : 4;
        for (int i = 0; i < len; i++) {
            retVal |= (bs[i] & 0xFF) << ((i & 0x03) << 3);
        }
        return retVal;
        // 如果确定足4位，可直接返回值
        // return (bs[0]&0xFF) | ((bs[1] & 0xFF)<<8) | ((bs[2] & 0xFF)<<16) | ((bs[3] & 0xFF)<<24);
    }

    @Override
    public List<Object> fromWire(byte[] input) {
        ByteArrayInputStream byteArrayInputStream = null;
        DataInputStream dataInputStream = null;

        //
        List<Object> skyeyeWebFlowLogList;
        try {
            byteArrayInputStream = new ByteArrayInputStream(input);
            dataInputStream = new DataInputStream(byteArrayInputStream);

            // （1）获取count, 4个字节，二进制数据，日志总数。
            // int count = dataInputStream.readInt();//
            byte[] countBytes = new byte[4];
            if (4 != dataInputStream.read(countBytes)) {
                return null;
            }
            int count = byte2Int(countBytes);
            // System.out.println("------------------------------Arrays.asList(input):" + Arrays.asList(input));
            // System.out.println("------------------------------count:" + count);

            //
            skyeyeWebFlowLogList = new ArrayList<Object>(count);

            // （2）获取日志数据, 长度与由count指定。
            for (int i = 0; i < count; i++) {
                // （2-1）获取len，4个字节，二进制数据，第1个日志长度。
                // int len = dataInputStream.readInt();
                byte[] lenBytes = new byte[4];
                if (4 != dataInputStream.read(lenBytes)) {
                    return null;
                }
                int len = byte2Int(lenBytes);
                // System.out.println("------------------------------i:" + i + ",len:" + len);

                // （2-1）获取data，日志数据，长度与由len0指定。
                byte[] dataBytes = new byte[len];
                int dataBytesLength = dataInputStream.read(dataBytes);
                if (dataBytesLength != len) {
                    // TODO:
                    // throw new RuntimeException("dataBytesLength != len");
                    return null;
                } else {
                    skyeyeWebFlowLogList.add(dataBytes);
                }
            }
            //
            return skyeyeWebFlowLogList;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != byteArrayInputStream) {
                try {
                    byteArrayInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != dataInputStream) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //
        return null;
    }
}
