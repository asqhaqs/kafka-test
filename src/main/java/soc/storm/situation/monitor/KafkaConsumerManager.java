
package soc.storm.situation.monitor;

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

    public KafkaConsumerManager(String topic) {
        logger.info("init KafkaConsumerManager [{}]", topic);
        this.topic = topic;
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());
    }

    private static Properties createConsumerConfig() {
        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "1010ad");
        // properties.put("enable.auto.commit", "false");
        properties.put("enable.auto.commit", "true");
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

            // Utils.sleep(500);

        } catch (Exception e) {
            logger.warn("KafkaConsumerTask error", e);
        } finally {
            // consumer.close();
        }
        System.out.println("-------------------------------------------------------------sumCount:" + sumCount);
    }

    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        consumer.wakeup();
    }

}
