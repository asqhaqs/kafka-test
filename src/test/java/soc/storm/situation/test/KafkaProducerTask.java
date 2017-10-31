
package soc.storm.situation.test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import soc.storm.situation.protocolbuffer.AddressBookProtos.DNS;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaProducerTask extends Thread {
    private final String topic;
    private final KafkaProducer<String, byte[]> producer;
    private final long totalCount;
    private static AtomicLong atomicLong = new AtomicLong(0);
    private static byte[] pbBytes = getPBBytes();
    private CountDownLatch allDone;

    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.24.2.155:9092,172.24.2.156:9092,172.24.2.157:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("batch.size", 16384);// default: 16384
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);// 32M
        properties.put("acks", "0");
        properties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy。
        properties.put("topic.properties.fetch.enable", "true");
        return properties;
    }

    public KafkaProducerTask(String topic, long totalCount, CountDownLatch allDone) {
        this.topic = topic;
        this.producer = new KafkaProducer<String, byte[]>(createConsumerConfig());
        this.totalCount = totalCount;
        this.allDone = allDone;

        System.out.println("---------------pbBytes.length: " + pbBytes.length);
    }

    @Override
    public void run() {
        while (atomicLong.incrementAndGet() < totalCount) {
            // Future<RecordMetadata> future = producer.send(new ProducerRecord<String, byte[]>(topic, null,
            // getPBBytes()));
            producer.send(new ProducerRecord<String, byte[]>(topic, null, pbBytes));

            // try {
            // Thread.sleep(1000);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
        }

        producer.close();
        allDone.countDown();
    }

    /**
     * 
     * @return
     */
    public static byte[] getPBBytes() {
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
        // System.out.println("-------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);
        // } catch (InvalidProtocolBufferException e) {
        // e.printStackTrace();
        // }

        return sensorLog.toByteArray();
    }

    public static class KafkaProducerExecutorServiceTask extends Thread {

        @Override
        public void run() {
            try {
                int threadCount = 10;
                long totalCount = 100000000;// 1000 0000
                CountDownLatch allDone = new CountDownLatch(threadCount);
                long begin = System.currentTimeMillis();

                ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadCount);
                for (int i = 0; i < threadCount; i++) {
                    // ty_dns ty_dns_inputtest
                    fixedThreadPool.execute(new KafkaProducerTask("ty_dns_inputtest", totalCount, allDone));
                }

                allDone.await();

                long end = System.currentTimeMillis();
                System.out.println("load done, use time: " + (end - begin) + "ms");

                //
                KafkaProducerTask.atomicLong = new AtomicLong(0);
                fixedThreadPool.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerExecutorServiceTask kafkaProducerExecutorService = new KafkaProducerExecutorServiceTask();
        kafkaProducerExecutorService.start();
    }

}
