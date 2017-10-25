
package soc.storm.situation.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import soc.storm.situation.protocolbuffer.AddressBookProtos.DNS;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaProducerTask extends Thread {
    private String topic;
    private KafkaProducer<String, byte[]> producer;
    private final long perSecondCount;
    private static AtomicLong atomicLong = new AtomicLong(0);
    private static byte[] pbBytes = getPBBytes();

    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.24.2.155:9092,172.24.2.156:9092,172.24.2.157:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("batch.size", 1);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);// 32M
        properties.put("acks", "0");
        properties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy。
        properties.put("topic.properties.fetch.enable", "true");
        return properties;
    }

    public KafkaProducerTask(String topic, long perSecondCount) {
        this.topic = topic;
        this.producer = new KafkaProducer<String, byte[]>(createConsumerConfig());
        this.perSecondCount = perSecondCount;
    }

    @Override
    public void run() {
        long begin = System.currentTimeMillis();
        while (atomicLong.incrementAndGet() < perSecondCount) {
            // Future<RecordMetadata> future = producer.send(new ProducerRecord<String, byte[]>(topic, null,
            // getPBBytes()));
            producer.send(new ProducerRecord<String, byte[]>(topic, null, pbBytes));

            // if (0 == atomicLong.incrementAndGet() % perSecondCount) {
            // try {
            // Thread.sleep(1000);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
            // }

            // try {
            // Thread.sleep(1000);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
        }
        long end = System.currentTimeMillis();
        System.out.println("load done, use time: " + (end - begin) + "ms");
    }

    /**
     * 
     * @return
     */
    public static byte[] getPBBytes() {
        SENSOR_LOG.Builder sensorLogBuilder = SENSOR_LOG.newBuilder();

        DNS.Builder builder = DNS.newBuilder();
        builder.setDip("114.114.114.114");
        builder.setDport(1);
        builder.setSerialNum("serial_num");
        builder.setSport(1);
        builder.setAccessTime("aaa");
        builder.setDnsType(1);
        builder.setHost("host");
        DNS dns = builder.build();

        sensorLogBuilder.setSkyeyeDns(dns);
        sensorLogBuilder.setMessageType(2);// TODO:???
        SENSOR_LOG sensorLog = sensorLogBuilder.build();

        // try {
        // SENSOR_LOG log = SENSOR_LOG.parseFrom(sensorLog.toByteArray());
        // Object skyeyeWebFlowLogPB = log.getSkyeyeDns();
        // String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);
        // System.out.println("-------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);
        // } catch (InvalidProtocolBufferException e) {
        // e.printStackTrace();
        // }

        return sensorLog.toByteArray();
    }

    public static void main(String[] args) {
        int threadCount = 10;
        long perSecondCount = 1000000;

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            fixedThreadPool.execute(new KafkaProducerTask("ty_dns", perSecondCount));
        }

        fixedThreadPool.shutdown();
    }
}
