package soc.storm.situation.monitor.extend.gatherdalian;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import soc.storm.situation.contants.SystemMapEnrichConstants;

public class KafkaConsumerManager {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerManager.class);
	private KafkaConsumer<String, String> consumer;
	private final String topic;
	
    static {
        System.out.println("--------------------KafkaConsumerManager-------------SystemMapEnrichConstants.BROKER_URL:" + SystemMapEnrichConstants.BROKER_URL);
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
            SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }
	
	public KafkaConsumerManager(String topicInput) {
		logger.info("init KafkaConsumerManager [{}]", topicInput);
		this.topic = topicInput;
		this.consumer = new KafkaConsumer<String,String>(createConsumerConfig());
	}
	
	private static Properties createConsumerConfig() {
		logger.info("init createConsumerConfig");
		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", SystemMapEnrichConstants.BROKER_URL);
		properties.put("group.id", SystemMapEnrichConstants.KAFKA_CONSUMER_GROUP_ID_3052);
		properties.put("enable.auto.commit", "false");
		properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", SystemMapEnrichConstants.KAFKA_CONSUMER_AUTO_OFFSET_RESET_GYWA3061);
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        // kerberos 授权&认证
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.kerberos.service.name", "kafka");
        }
		
		return properties;
	}
	
	public void run(SpoutOutputCollector collector) {
		int sumCount = 0;
		
		try {
			this.consumer.subscribe(Arrays.asList(topic));
			while(true) {
				ConsumerRecords<String,String> records = consumer.poll(100);
				sumCount += records.count();
				consumer.commitSync();
				for(ConsumerRecord<String, String> record : records) {
					collector.emit(new Values(record.value()));
				}
			}
			
		}catch(Exception e) {
			logger.error("--------------------------KafkaConsumerTask error!");
		}finally {
			
		}
		logger.info("-----------------------------------consumer msg  sumCount: {}", sumCount);
	}
	
	
    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        consumer.wakeup();
    }

}
