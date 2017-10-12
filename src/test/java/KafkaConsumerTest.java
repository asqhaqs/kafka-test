import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTest extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public KafkaConsumerTest(String topic) {
        this.consumer = new KafkaConsumer<String, String>(createConsumerConfig());
        this.topic = topic;
    }

    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.24.2.155:9092,172.24.2.156:9092,172.24.2.157:9092");
        // properties.put("group.id", SystemConstants.TOPOLOGY_NAME);
        properties.put("group.id", "zhongsanmu111");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        // properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        properties.put("auto.offset.reset", "latest");// String must be one of: latest, earliest, none
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        long begin = System.currentTimeMillis();
        long sumCount = 0;
        while (true) {
            //
            ConsumerRecords<String, String> records = consumer.poll(100);
            sumCount += records.count();
            System.out.println("--------records.count():" + records.count());
            for (ConsumerRecord<String, String> consumerRecord : records) {
                System.out.println(String.format("offset=%d,key=%s,value=%s",
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.value()));
                break;
            }
            consumer.commitSync();

            // try {
            // Thread.sleep(100000);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }

            long end = System.currentTimeMillis();
            if (end - begin > 10000) {
                break;
            }
        }
        System.out.println("-------------------sumCount:" + sumCount);

        // KafkaConsumer c = new KafkaConsumer(props);
        // Map records = c.poll(1000);
        // c.commit(true);

        // KafkaConsumer c = new KafkaConsumer(properties);
        // ConsumerRecords records = c.poll(1000);
        // c.commitSync();
    }

    public static void main(String[] args) {
        KafkaConsumerTest consumerTest = new KafkaConsumerTest("ty_tcpflow");// ty_tcpflow ty_dns
        consumerTest.start();
    }

}
