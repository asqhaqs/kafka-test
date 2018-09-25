
package soc.storm.situation.monitor.extend.compress3052;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.utils.FileUtil;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;

/**
 * KafkaConsumerManager
 * 
 * @author wangbin03
 *
 */
public class KafkaConsumerManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final KafkaConsumer<String, Object> consumer;

    private final String topic;

    static {
        System.out.println("--------------------KafkaConsumerManager-------------SystemConstants.BROKER_URL:" + SystemConstants.BROKER_URL);
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
                SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }

    public KafkaConsumerManager(String topic) {
        logger.info("init KafkaConsumerManager [{}]", topic);
        this.topic = topic;
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());
    }

    private static Properties createConsumerConfig() {
        try {
            FileUtil.testConfigFile("KafkaConsumerManager");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        // properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "30523113357");
        properties.put("group.id", SystemConstants.KAFKA_CONSUMER_GROUP_ID_3052);
        properties.put("enable.auto.commit", "false");
        // properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        // properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        // String must be one of: latest, earliest, none
        properties.put("auto.offset.reset", SystemConstants.KAFKA_CONSUMER_AUTO_OFFSET_RESET_3052);
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        // kerberos 授权&认证
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.kerberos.service.name", "kafka");
        }

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
