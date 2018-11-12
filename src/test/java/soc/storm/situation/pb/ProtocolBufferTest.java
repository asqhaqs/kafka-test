
package soc.storm.situation.pb;

// import soc.storm.situation.protocolbuffer.AddressBookProtos.DNS;
// import soc.storm.situation.protocolbuffer.AddressBookProtos.TCPFLOW;

import soc.storm.situation.protocolbuffer.AddressBookProtos.DNS;
import soc.storm.situation.protocolbuffer.AddressBookProtos.TCPFLOW;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 * @author wangbin03
 *
 */
public class ProtocolBufferTest {

    public static void main(String[] args) throws InvalidProtocolBufferException {
        // testTCPFLOW();
        testDNS();
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

    public static void testTCPFLOW() throws InvalidProtocolBufferException {
        // 将模拟对象转成byte[]，方便传输
        TCPFLOW.Builder builder = TCPFLOW.newBuilder();
        builder.setDip("114.114.114.114");
        builder.setDport(1);
        // serial_num, status, stime, dtime, sport, proto, uplink_length, downlink_length, client_os, server_os,
        // src_mac, dst_mac, up_payload, down_payload, summary
        builder.setSerialNum("serial_num");
        builder.setStatus("status");
        builder.setStime("111");
        builder.setDtime("aa");
        builder.setDtime("");

        builder.setSport(1);
        builder.setProto("");
        builder.setUplinkLength(1L);
        builder.setDownlinkLength(1L);
        builder.setClientOs("");
        builder.setServerOs("");

        builder.setSrcMac("");
        builder.setDstMac("");
        builder.setUpPayload("");
        builder.setDownPayload("");
        builder.setSummary("");

        TCPFLOW tcpflow = builder.build();
        System.out.println("------------before:\n" + tcpflow.toString());

        for (byte b : tcpflow.toByteArray()) {
            System.out.print(b + " ");
        }

        System.out.println();
        System.out.println("------------tcpflow.toByteArray().length:" + tcpflow.toByteArray().length);
        System.out.println(tcpflow.toByteString());
        System.out.println("==============================");

        // 模拟接受Byte[]，发序列化成Person类
        byte[] byteArray = tcpflow.toByteArray();
        TCPFLOW p2 = TCPFLOW.parseFrom(byteArray);
        System.out.println("after:\n" + p2.toString());
    }
}