package cn.situation.jobs;

import cn.situation.cons.SystemConstant;
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
	private KafkaProducer<String, byte[]> producer;
	private long totalCount;

	static {
		if (SystemConstant.IS_KERBEROS.equals("true")) {
			System.setProperty("java.security.auth.login.config",
					SystemConstant.GEO_DATA_PATH + File.separator + "kafka_server_jaas.conf");
			System.setProperty("java.security.krb5.conf", SystemConstant.GEO_DATA_PATH + File.separator + "krb5.conf");
		}
	}

	private static Properties createProducerConfig() {
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("bootstrap.servers", SystemConstant.BROKER_URL);
		kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		kafkaProducerProperties.put("batch.size", 1);
		kafkaProducerProperties.put("linger.ms", 1);
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
		this.producer = new KafkaProducer<>(createProducerConfig());
		this.totalCount = totalCount;
	}

	@Override
	public void run() {
		int count = 1;
		String fileName = SystemConstant.GEO_DATA_PATH + File.separator + topicName + ".txt";
		LOG.info(String.format("[%s]: topicName<%s>, fileName<%s>", "run", topicName, fileName));
		while (count <= totalCount) {
			File file = new File(fileName);
			if (!file.exists() || !file.isFile() || file.length() == 0) {
				LOG.warn(String.format("[%s]: topicName<%s>, message<%s>", "run", topicName, "file not exists."));
				return;
			}
			FileInputStream fileInputStream = null;
			InputStreamReader inputStreamReader = null;
			BufferedReader reader = null;
			try {
				fileInputStream = new FileInputStream(file);
				inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
				reader = new BufferedReader(inputStreamReader, Integer.parseInt(SystemConstant.INPUT_BUFFER_SIZE));
				String line;
				int i = 0;
				while ((line = reader.readLine()) != null) {
					i++;
					producer.send(new ProducerRecord<>(topicName, null, line.getBytes()));
				}
				LOG.info(String .format("[%s]: topicName<%s>, fileName<%s>, line<%s>", "run", topicName, fileName, i));
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			} finally {
				try {
					if (null != reader) {
						reader.close();
					}
					if (null != inputStreamReader) {
						inputStreamReader.close();
					}
					if (null != fileInputStream) {
						fileInputStream.close();
					}
				} catch (Exception ie) {
					LOG.error(ie.getMessage(), ie);
				}
			}
			LOG.info(String.format("[%s]: topicName<%s>, totalCount<%s>, count<%s>", "run", topicName, totalCount, count));
			count++;
		}
		producer.close();
	}
}