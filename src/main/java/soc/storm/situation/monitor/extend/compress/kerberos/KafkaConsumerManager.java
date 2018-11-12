
package soc.storm.situation.monitor.extend.compress.kerberos;

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

    // static {
    // System.out.println("--------------------KafkaConsumerManager-------------SystemConstants.BROKER_URL:" +
    // SystemConstants.BROKER_URL);
    // System.setProperty("java.security.auth.login.config",
    // SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
    // System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator +
    // "krb5.conf");
    // }

    public KafkaConsumerManager(String topic) {
        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");

        logger.info("init KafkaConsumerManager [{}]", topic);
        this.topic = topic;
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());
    }

    private static Properties createConsumerConfig() {
        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");
        try {
            FileUtil.testConfigFile("KafkaConsumerManager");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "k126903");
        properties.put("enable.auto.commit", "false");
        // properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        // properties.put("auto.offset.reset", "latest");// String must be one of: latest, earliest, none
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
        try {
            this.consumer.subscribe(Arrays.asList(topic));
            ConsumerRecords<String, Object> records = consumer.poll(100);
            consumer.commitSync();
            for (ConsumerRecord<String, Object> consumerRecord : records) {
                System.out.println("--------------KafkaConsumerManager.run:" + consumerRecord.value());
                collector.emit(new Values(consumerRecord.value()));
            }
        } catch (Exception e) {
            logger.warn("KafkaConsumerTask error", e);
        } finally {
            // consumer.close();
        }
    }

    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        consumer.wakeup();
    }

}