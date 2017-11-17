
package soc.storm.situation.coder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author wangbin03
 *
 */
public class WebFlowLogGatherMsgSerializableCoder implements WebFlowLogGatherMsgCoder {

    @SuppressWarnings("unchecked")
    @Override
    public List<Object> fromWire(byte[] input) {
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(input);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);

            ArrayList<Object> pbBytesWebFlowLogList = (ArrayList<Object>) objectInputStream.readObject();

            return pbBytesWebFlowLogList;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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
            if (null != objectInputStream) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }
}
