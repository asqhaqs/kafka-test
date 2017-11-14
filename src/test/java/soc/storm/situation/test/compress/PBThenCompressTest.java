
package soc.storm.situation.test.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import soc.storm.situation.compress.Lz4Compress;
import soc.storm.situation.compress.LzoCompress;
import soc.storm.situation.compress.SnappyCompress;
import soc.storm.situation.protocolbuffer.AddressBookProtos.DNS;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.protocolbuffer.AddressBookProtos.TCPFLOW;

/**
 * 
 * @author wangbin03
 *
 */
public class PBThenCompressTest {

    //
    private static byte[] pbBytesDNS = getPBBytesDNS();// 113B
    private static byte[] pbBytesTcpFlow = getPBBytesTcpFlow();// 500B

    /**
     * 
     * @author wangbin03
     *
     */
    public static class PBContentDNS {
        final static int arrayListLength = 8850;
        public static List<byte[]> pbBytesDNSList = new ArrayList<byte[]>();
        public static byte[] pbBytesDNSByteArray = {};

        static {
            for (int i = 0; i < arrayListLength; i++) {
                pbBytesDNSList.add(pbBytesDNS.clone());
            }
            //
            try {
                pbBytesDNSByteArray = getByteArray(pbBytesDNSList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static int getSize() throws IOException {
            return size(pbBytesDNSList);
        }
    }

    /**
     * 
     * @author wangbin03
     *
     */
    public static class PBContentTcpFlow {
        // final static int arrayListLength = 2000;
        // final static int arrayListLength = 100;
        final static int arrayListLength = 10;
        public static List<byte[]> pbBytesTcpFlowList = new ArrayList<byte[]>();
        public static byte[] pbBytesTcpFlowByteArray = {};

        static {
            for (int i = 0; i < arrayListLength; i++) {
                pbBytesTcpFlowList.add(pbBytesTcpFlow.clone());
            }
            //
            try {
                pbBytesTcpFlowByteArray = getByteArray(pbBytesTcpFlowList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static int getSize() throws IOException {
            return size(pbBytesTcpFlowList);
        }

    }

    /**
     * java Serializable
     * 
     * @param o
     * @return
     * @throws IOException
     */
    public static int size(final Object o) throws IOException {
        if (o == null) {
            return 0;
        }

        ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
        ObjectOutputStream out = new ObjectOutputStream(buf);
        out.writeObject(o);
        out.flush();
        buf.close();
        return buf.size();
    }

    /**
     * java Serializable
     * 
     * @param o
     * @return
     * @throws IOException
     */
    public static byte[] getByteArray(final Object o) throws IOException {
        byte[] resultByteArray = {};
        if (o == null) {
            return resultByteArray;
        }

        ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
        ObjectOutputStream out = new ObjectOutputStream(buf);
        out.writeObject(o);
        out.flush();
        buf.close();
        return buf.toByteArray();
    }

    public static void main(String[] args) throws IOException {
        PBThenCompressTest pbTest = new PBThenCompressTest();
        // dns
        System.out.println("-----PBContentDNS.getSize():" + PBContentDNS.getSize());
        pbTest.pBCompressByLZO(PBContentDNS.pbBytesDNSByteArray);
        pbTest.pBCompressBySnappy(PBContentDNS.pbBytesDNSByteArray);
        pbTest.pBCompressByLz4(PBContentDNS.pbBytesDNSByteArray);

        System.out.println("\n");

        // tcpflow
        System.out.println("-----PBContentTcpFlow.getSize():" + PBContentTcpFlow.getSize());
        pbTest.pBCompressByLZO(PBContentTcpFlow.pbBytesTcpFlowByteArray);
        pbTest.pBCompressBySnappy(PBContentTcpFlow.pbBytesTcpFlowByteArray);
        pbTest.pBCompressByLz4(PBContentTcpFlow.pbBytesTcpFlowByteArray);

        // -----PBContentDNS.getSize():1265621
        // -----pBCompressByLZO--compressByteArray.length:898881
        // -----pBCompressBySnappy--compressByteArray.length:62087
        // -----pBCompressByLz4--compressByteArray.length:30973
        //
        //
        // -----PBContentTcpFlow.getSize():1020071
        // -----pBCompressByLZO--compressByteArray.length:1021696
        // -----pBCompressBySnappy--compressByteArray.length:55123
        // -----pBCompressByLz4--compressByteArray.length:65656
    }

    public void pBCompressByLZO(byte[] sourceByteArray) {
        byte[] compressByteArray = LzoCompress.compress(sourceByteArray);
        System.out.println("-----pBCompressByLZO--compressByteArray.length:" + compressByteArray.length);

        //
        // byte[] deCommpressByteArray = LzoTest.deCommpress001(compressByteArray);
        // System.out.println("-----pBCompressByLZO--deCommpressByteArray.length:" +
        // deCommpressByteArray.length);
    }

    public void pBCompressBySnappy(byte[] sourceByteArray) {
        byte[] compressByteArray = SnappyCompress.compress(sourceByteArray);
        System.out.println("-----pBCompressBySnappy--compressByteArray.length:" + compressByteArray.length);

        //
        // byte[] deCommpressByteArray = SnappyTest.deCommpress001(compressByteArray);
        // System.out.println("-----pBCompressBySnappy--deCommpressByteArray.length:" +
        // deCommpressByteArray.length);
    }

    public void pBCompressByLz4(byte[] sourceByteArray) {
        byte[] compressByteArray = Lz4Compress.compress(sourceByteArray);
        System.out.println("-----pBCompressByLz4--compressByteArray.length:" + compressByteArray.length);
    }

    public void pBCompressByAvro(byte[] sourceByteArray) {

    }

    /**
     * 生成DNS类型的PB测试数据
     * 
     * @return
     */
    public static byte[] getPBBytesDNS() {
        SENSOR_LOG.Builder sensorLogBuilder = SENSOR_LOG.newBuilder();

        DNS.Builder builder = DNS.newBuilder();
        builder.setDip("114.114.114.114");
        builder.setDport(43423);
        builder.setSerialNum("214246597");
        builder.setSport(53);
        builder.setAccessTime("2017-09-05 16:37:57.334");
        builder.setDnsType(1);
        builder.setHost("www.google-analytics.com");
        builder.setCount("1;3;0;0");
        builder.setReplyCode(0);
        builder.addAddr("119.37.197.93");
        builder.addAddr("183.131.1.125");
        DNS dns = builder.build();

        sensorLogBuilder.setSkyeyeDns(dns);
        sensorLogBuilder.setMessageType(2);// start with TCPFLOW:1;DNS:2;.....
        SENSOR_LOG sensorLog = sensorLogBuilder.build();

        // try {
        // SENSOR_LOG log = SENSOR_LOG.parseFrom(sensorLog.toByteArray());
        // Object skyeyeWebFlowLogPB = log.getSkyeyeDns();
        // String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);
        // System.out.println("-------------------sensorLog.toByteArray().length:" + sensorLog.toByteArray().length);//
        // 133
        // System.out.println("-------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);
        // System.out.println("-------------------skyeyeWebFlowLogStr.getBytes().length:" +
        // skyeyeWebFlowLogStr.getBytes().length);// 246
        // } catch (InvalidProtocolBufferException e) {
        // e.printStackTrace();
        // }

        return sensorLog.toByteArray();
    }

    /**
     * 生成TcpFlow类型的PB测试数据
     * 
     * @return
     */
    public static byte[] getPBBytesTcpFlow() {
        SENSOR_LOG.Builder sensorLogBuilder = SENSOR_LOG.newBuilder();

        TCPFLOW.Builder builder = TCPFLOW.newBuilder();
        builder.setDip("114.114.114.114");
        builder.setDport(1);
        builder.setSip("11.8.45.37");
        builder.setSport(11737);
        // serial_num, status, stime, dtime, sport, proto, uplink_length, downlink_length, client_os, server_os,
        // src_mac, dst_mac, up_payload, down_payload, summary
        builder.setSerialNum("215332105");
        builder.setStatus("fin");
        builder.setStime("2016-04-06 11:36:57.099");
        builder.setDtime("2016-04-06 11:36:57.114");

        builder.setProto("http");
        builder.setUplinkLength(424L);
        builder.setDownlinkLength(154L);
        builder.setClientOs("Linux2.2.x-3.x (no timestamps)");
        builder.setServerOs("Linux3.x");

        builder.setSrcMac("fa:16:3e:21:59:41");
        builder.setDstMac("5c:dd:70:94:bd:00");
        // builder.setUpPayload("474554202f73686f756a692f6d6f766965737461743f616374696f6e3d656e746572266d5f763d312e312e30266d69643d3836353732383032303938303131362666726f6d3d2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f77696e6e74");
        builder.setUpPayload("474554202f73686f756a692f6d6f766965737461743f616374696f6e3d656e746572266d5f763");
        builder.setDownPayload("485454502f312e3120323030204f4b0d0a53657276657200206e67696e782f312e322e390d0a4461746500205765642c2030362041707220323031362030333a32303a313720474d540d0a436f6e74656e742d547970650020746578742f68746d6c0d0a");
        builder.setSummary("53;0;1460;1452");

        TCPFLOW tcpflow = builder.build();

        sensorLogBuilder.setSkyeyeTcpflow(tcpflow);
        sensorLogBuilder.setMessageType(1);// start with TCPFLOW:1;DNS:2;.....
        SENSOR_LOG sensorLog = sensorLogBuilder.build();

        // try {
        // SENSOR_LOG log = SENSOR_LOG.parseFrom(sensorLog.toByteArray());
        // Object skyeyeWebFlowLogPB = log.getSkyeyeTcpflow();
        // String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);
        // System.out.println("-------------------sensorLog.toByteArray().length:" + sensorLog.toByteArray().length);//
        // 500
        // System.out.println("-------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);
        // System.out.println("-------------------skyeyeWebFlowLogStr.getBytes().length:" +
        // skyeyeWebFlowLogStr.getBytes().length);// 717
        // } catch (InvalidProtocolBufferException e) {
        // e.printStackTrace();
        // }

        return sensorLog.toByteArray();
    }

}
