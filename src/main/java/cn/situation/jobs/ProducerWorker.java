package cn.situation.jobs;

import cn.situation.cons.SystemConstant;
import cn.situation.util.FileUtil;
import cn.situation.util.LogUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerWorker implements Runnable {

	private static final Logger LOG = LogUtil.getInstance(ProducerWorker.class);
	private String topicName;
	private static KafkaProducer<String, byte[]> producer;
	private long totalCount;

	static {
		if (SystemConstant.IS_KERBEROS.equals("true")) {
			System.setProperty("java.security.auth.login.config",
					SystemConstant.GEO_DATA_PATH + File.separator + "kafka_server_jaas.conf");
			System.setProperty("java.security.krb5.conf", SystemConstant.GEO_DATA_PATH + File.separator + "krb5.conf");
		}
		producer = new KafkaProducer<>(createProducerConfig());
	}

	private static Properties createProducerConfig() {
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("bootstrap.servers", SystemConstant.BROKER_URL);
		kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		kafkaProducerProperties.put("batch.size", 16384);
		kafkaProducerProperties.put("buffer.memory", 33554432);
		kafkaProducerProperties.put("acks", "0");
		kafkaProducerProperties.put("compression.type", "snappy");
		kafkaProducerProperties.put("topic.properties.fetch.enable", "true");
		if (SystemConstant.IS_KERBEROS.equals("true")) {
			kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
			kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
		}
		return kafkaProducerProperties;
	}

	public ProducerWorker(String topicName, long totalCount) {
		LOG.info(String.format("[%s]: topicName<%s>, totalCount<%s>", "ProducerWorker", topicName, totalCount));
		this.topicName = topicName;
		this.totalCount = totalCount;
	}

	@Override
	public void run() {
		String fileName = SystemConstant.GEO_DATA_PATH + File.separator + topicName + ".txt";
		LOG.info(String.format("[%s]: topicName<%s>, fileName<%s>", "run", topicName, fileName));
		byte[] data = null;
		try {
			data = FileUtil.getContent(fileName);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		int count = 1;
		while (count <= totalCount) {
			producer.send(new ProducerRecord<>(topicName, null, data));
			LOG.info(String.format("[%s]: topicName<%s>, totalCount<%s>, count<%s>", "run", topicName, totalCount, count));
			count++;
		}
	}
}