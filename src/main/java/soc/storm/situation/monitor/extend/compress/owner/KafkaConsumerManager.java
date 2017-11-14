
package soc.storm.situation.monitor.extend.compress.owner;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaConsumerManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final KafkaConsumer<String, Object> consumer;

    private final String topic;

    // private static byte[] value = { 8, 1, 18, -17, 3, 10, 9, 50, 49, 53, 51, 51, 50, 49, 48, 53, 18, 3, 102, 105,
    // 110, 26, 23, 50, 48, 49, 54, 45, 48, 52, 45, 48, 54, 32, 49, 49, 58, 51, 54, 58, 53, 55, 46, 48, 57, 57, 34, 23,
    // 50, 48, 49, 54, 45, 48, 52, 45, 48, 54, 32, 49, 49, 58, 51, 54, 58, 53, 55, 46, 49, 49, 52, 42, 10, 49, 49, 46,
    // 56, 46, 52, 53, 46, 51, 55, 56, -39, 91, 66, 15, 49, 49, 52, 46, 49, 49, 52, 46, 49, 49, 52, 46, 49, 49, 52, 80,
    // 1, 90, 4, 104, 116, 116, 112, 96, -88, 3, 104, -102, 1, 114, 30, 76, 105, 110, 117, 120, 50, 46, 50, 46, 120, 45,
    // 51, 46, 120, 32, 40, 110, 111, 32, 116, 105, 109, 101, 115, 116, 97, 109, 112, 115, 41, 122, 8, 76, 105, 110,
    // 117, 120, 51, 46, 120, -126, 1, 17, 102, 97, 58, 49, 54, 58, 51, 101, 58, 50, 49, 58, 53, 57, 58, 52, 49, -118,
    // 1, 17, 53, 99, 58, 100, 100, 58, 55, 48, 58, 57, 52, 58, 98, 100, 58, 48, 48, -110, 1, 77, 52, 55, 52, 53, 53,
    // 52, 50, 48, 50, 102, 55, 51, 54, 56, 54, 102, 55, 53, 54, 97, 54, 57, 50, 102, 54, 100, 54, 102, 55, 54, 54, 57,
    // 54, 53, 55, 51, 55, 52, 54, 49, 55, 52, 51, 102, 54, 49, 54, 51, 55, 52, 54, 57, 54, 102, 54, 101, 51, 100, 54,
    // 53, 54, 101, 55, 52, 54, 53, 55, 50, 50, 54, 54, 100, 53, 102, 55, 54, 51, -102, 1, -56, 1, 52, 56, 53, 52, 53,
    // 52, 53, 48, 50, 102, 51, 49, 50, 101, 51, 49, 50, 48, 51, 50, 51, 48, 51, 48, 50, 48, 52, 102, 52, 98, 48, 100,
    // 48, 97, 53, 51, 54, 53, 55, 50, 55, 54, 54, 53, 55, 50, 48, 48, 50, 48, 54, 101, 54, 55, 54, 57, 54, 101, 55, 56,
    // 50, 102, 51, 49, 50, 101, 51, 50, 50, 101, 51, 57, 48, 100, 48, 97, 52, 52, 54, 49, 55, 52, 54, 53, 48, 48, 50,
    // 48, 53, 55, 54, 53, 54, 52, 50, 99, 50, 48, 51, 48, 51, 54, 50, 48, 52, 49, 55, 48, 55, 50, 50, 48, 51, 50, 51,
    // 48, 51, 49, 51, 54, 50, 48, 51, 48, 51, 51, 51, 97, 51, 50, 51, 48, 51, 97, 51, 49, 51, 55, 50, 48, 52, 55, 52,
    // 100, 53, 52, 48, 100, 48, 97, 52, 51, 54, 102, 54, 101, 55, 52, 54, 53, 54, 101, 55, 52, 50, 100, 53, 52, 55, 57,
    // 55, 48, 54, 53, 48, 48, 50, 48, 55, 52, 54, 53, 55, 56, 55, 52, 50, 102, 54, 56, 55, 52, 54, 100, 54, 99, 48,
    // 100, 48, 97, -94, 1, 14, 53, 51, 59, 48, 59, 49, 52, 54, 48, 59, 49, 52, 53, 50 };

    public KafkaConsumerManager(String topic) {
        logger.info("init KafkaConsumerManager [{}]", topic);
        this.topic = topic;
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());
    }

    private static Properties createConsumerConfig() {
        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "1147");
        properties.put("enable.auto.commit", "false");
        // properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        // properties.put("auto.offset.reset", "latest");// String must be one of: latest, earliest, none
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // properties.put("partition.assignment.strategy", "range");
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");

        return properties;
    }

    // push消费方式，服务端推送过来。主动方式是pull
    public void run(SpoutOutputCollector collector) {
        long sumCount = 0;

        try {
            this.consumer.subscribe(Arrays.asList(topic));
            ConsumerRecords<String, Object> records = consumer.poll(100);
            sumCount += records.count();
            consumer.commitSync();
            for (ConsumerRecord<String, Object> consumerRecord : records) {
                // System.out.println(String.format("--------------------offset=%d,key=%s,value=%s",
                // consumerRecord.offset(),
                // consumerRecord.key(),
                // consumerRecord.value()));

                // String messageId = UUID.randomUUID().toString().replaceAll("-", "");
                // collector.emit(new Values(consumerRecord.value()), messageId);
                collector.emit(new Values(consumerRecord.value()));
            }

            // for (int i = 0; i < 1; i++) {
            // collector.emit(new Values(value));
            // }

            // collector.emit(new Values(value));
            // sumCount = 1;

            // Utils.sleep(500);
        } catch (Exception e) {
            logger.warn("KafkaConsumerTask error", e);
        } finally {
            // consumer.close();
        }
        // System.out.println("-------------------------------------------------------------sumCount:" + sumCount);
    }

    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        consumer.wakeup();
    }

}
