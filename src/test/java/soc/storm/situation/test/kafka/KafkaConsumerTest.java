
package soc.storm.situation.test.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import soc.storm.situation.contants.SystemConstants;

public class KafkaConsumerTest extends Thread {
    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;

    public KafkaConsumerTest(String topic) {
        this.consumer = new KafkaConsumer<String, byte[]>(createConsumerConfig());
        this.topic = topic;
    }

    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "ABC12410");
        properties.put("enable.auto.commit", "false");
        // properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        // properties.put("auto.offset.reset", "latest");// String must be one of: latest, earliest, none
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        // kerberos 授权&认证
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.kerberos.service.name", "kafka");
        }

        return properties;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        long begin = System.currentTimeMillis();
        long sumCount = 0;
        while (true) {
            //
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            sumCount += records.count();
            System.out.println("--------records.count():" + records.count());
            for (ConsumerRecord<String, byte[]> consumerRecord : records) {
                System.out.println(String.format("offset=%d,key=%s,value=%s",
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.value().length));
                break;
            }
            consumer.commitSync();

            // try {
            // Thread.sleep(100000);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }

            long end = System.currentTimeMillis();
            if (end - begin > 1000) {
                break;
            }
        }
        System.out.println("-------------------sumCount:" + sumCount);
    }

    /**
     * 获取协议包
     * 
     * @return
     */
    public byte[] getRecordValue() {
        byte[] result = null;
        boolean isBreak = false;
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            //
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            if (0 < records.count()) {
                System.out.println("--------records.count():" + records.count());
                for (ConsumerRecord<String, byte[]> consumerRecord : records) {
                    result = consumerRecord.value();
                    isBreak = true;
                    break;
                }
            }
            consumer.commitSync();

            if (isBreak) {
                break;
            }
        }

        return result;
    }

    public static void main(String[] args) {
        KafkaConsumerTest consumerTest = new KafkaConsumerTest("ty_tcp");
        // KafkaConsumerTest consumerTest = new KafkaConsumerTest("skyeye_ssl");// skyeye_ssl
        consumerTest.start();

        // byte[] result = consumerTest.getRecordValue();
        // System.out.println("-----------result.length:" + result.length);
    }

}
