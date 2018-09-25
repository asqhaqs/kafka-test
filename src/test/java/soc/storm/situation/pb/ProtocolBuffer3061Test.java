
package soc.storm.situation.pb;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.protocolbuffer.AddressBookProtos3061.DNS;
import soc.storm.situation.protocolbuffer.AddressBookProtos3061.SENSOR_LOG;
import soc.storm.situation.utils.BytesHexStrTranslate;
import soc.storm.situation.utils.JsonFormatProtocolBuffer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * 
 * @author wangbin03
 *
 */
public class ProtocolBuffer3061Test {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolBuffer3061Test.class);

    public static void main(String[] args) throws InvalidProtocolBufferException {
        // testDNS();
        testDNS3061();
    }

    public static void testDNS3061() {
        try {
            // 081a8201b5030a093231343234363539341217323031382d30332d33302031343a31323a35342e3838341a17323031382d30332d33302031343a31343a35352e3139332a27666538303a303030303a303030303a303030303a633937653a393865613a653035393a3465383330a2044227666630323a303030303a303030303a303030303a303030303a303030303a303030313a3030303248a304507358006001680072c80130313062623931653030303830303032303030303030303130303065303030313030303131656565656537356330336664353931303030353030303330303063306563303366643530303030303030303030303030303030303032373030323530303065366236313665363737333638363136663661363936353264343433313035363936653734373236313039366336353637363536653634373336353633303336333666366430303030313030303065303030303031333730303038346435333436353432307a0082011163303a33663a64353a39313a30303a30358a011133333a33333a30303a30313a30303a30329201066468637076369a0117323031382d30332d33302031343a31343a35352e313933

            Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;
            Method getSkyeyeWebFlowLogObjectMethod;

            getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod("getSkyeyeDns");

            // 模拟接受Byte[]，发序列化成Person类
            byte[] skyeyeWebFlowLogByteArrayElementBytes = BytesHexStrTranslate
                    .toBytes("081a8201b5030a093231343234363539341217323031382d30332d33302031343a31323a35342e3838341a17323031382d30332d33302031343a31343a35352e3139332a27666538303a303030303a303030303a303030303a633937653a393865613a653035393a3465383330a2044227666630323a303030303a303030303a303030303a303030303a303030303a303030313a3030303248a304507358006001680072c80130313062623931653030303830303032303030303030303130303065303030313030303131656565656537356330336664353931303030353030303330303063306563303366643530303030303030303030303030303030303032373030323530303065366236313665363737333638363136663661363936353264343433313035363936653734373236313039366336353637363536653634373336353633303336333666366430303030313030303065303030303031333730303038346435333436353432307a0082011163303a33663a64353a39313a30303a30358a011133333a33333a30303a30313a30303a30329201066468637076369a0117323031382d30332d33302031343a31343a35352e313933");

            // byte[] skyeyeWebFlowLogByteArrayElementBytes = BytesHexStrTranslate
            // .toBytes("08021a790a093231343234363539341217323031382d30332d33302031343a31343a35332e3138301a0a31302e37342e312e31312835320b31302e37342e34312e34314091c403480150005a07313b313b303b30621a6871747130312e696e7472612e6c6567656e647365632e636f6d6a0b3137322e32342e302e3939");

            SENSOR_LOG log = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArrayElementBytes);

            System.out.println("enrichmentBolt-----------------------log.getMessageType:" + log.getMessageType());

            Object skyeyeWebFlowLogPB = getSkyeyeWebFlowLogObjectMethod.invoke(log);
            // String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);
            String skyeyeWebFlowLogStr = JsonFormatProtocolBuffer.printToString((Message) skyeyeWebFlowLogPB);

            System.out.println("enrichmentBolt-----------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);
        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog-", e);
        }

    }

    public static void testDNS() throws InvalidProtocolBufferException {
        // 将模拟对象转成byte[]，方便传输
        DNS.Builder builder = DNS.newBuilder();
        builder.setDip("114.114.114.114");
        builder.setDport(1);
        builder.setSerialNum("serial_num");
        builder.setSport(1);
        builder.setAccessTime("aaa");
        builder.setDnsType(1);
        builder.setHost("host");

        DNS dns = builder.build();
        System.out.println("------------before:\n" + dns.toString());

        for (byte b : dns.toByteArray()) {
            System.out.print(b + " ");
        }

        System.out.println();
        System.out.println("------------dns.toByteArray().length:" + dns.toByteArray().length);
        System.out.println(dns.toByteString());
        System.out.println("==============================");

        // 模拟接受Byte[]，发序列化成Person类
        byte[] byteArray = dns.toByteArray();
        DNS p2 = DNS.parseFrom(byteArray);
        System.out.println("after:\n" + p2.toString());
    }

}
