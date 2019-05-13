package cn.situation.jobs;

import cn.situation.cons.SystemConstant;
import cn.situation.util.FileUtil;
import cn.situation.util.LogUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerWorker implements Runnable {

	private static final Logger LOG = LogUtil.getInstance(ProducerWorker.class);
	private String topicName;
	private KafkaProducer<String, byte[]> producer;
	private List<Future<RecordMetadata>> kafkaFutures = new LinkedList<>();
	private int totalCount;
	private int maxAvroLen = 0;

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
		kafkaProducerProperties.put("batch.size", 16384);
		kafkaProducerProperties.put("buffer.memory", 33554432);
		kafkaProducerProperties.put("acks", "0");
		if (SystemConstant.IS_KERBEROS.equals("true")) {
			kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
			kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
		}
		return kafkaProducerProperties;
	}

	public ProducerWorker(String topicName, int totalCount) {
		LOG.info(String.format("[%s]: topicName<%s>, totalCount<%s>", "ProducerWorker", topicName, totalCount));
		this.topicName = topicName;
		this.totalCount = totalCount;
		producer = new KafkaProducer<>(createProducerConfig());
	}

	@Override
	public void run() {
		String fileName = SystemConstant.GEO_DATA_PATH + File.separator + topicName + ".txt";
		LOG.info(String.format("[%s]: topicName<%s>, fileName<%s>", "run", topicName, fileName));
		try {
			sendMessage(fileName, 10000, totalCount);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void sendMessage(String file, int num, int limit) throws Exception {
		FileInputStream in = new FileInputStream(file);
		long n = System.nanoTime();
		kafkaFutures.clear();

		int c = 0;
		int avc = 0;
		long size = 0;
		int maxPack = 0;
		int maxC = 0;
		long t = System.currentTimeMillis();
		try {
			while(in.available()>8&&(c<limit||limit<=0)){
				byte[] sendData = readNextMessage(in);
				int cc = checkValid(sendData);
				if (cc>0) {
					if (maxC<cc)
						maxC = cc;
					avc += cc;
					kafkaFutures.add(producer.send(new ProducerRecord<>(topicName, null, sendData)));
					c++;
					if (sendData.length>maxPack)
						maxPack = sendData.length;
					size += sendData.length;
					if (c % num == 0) {
						producer.flush();
						for (Future<RecordMetadata> future : kafkaFutures) {
							future.get();
						}
						kafkaFutures.clear();
						LOG.info("Send " + c + " messages " + " size " + size + " logs " +avc +  " in " + (System.currentTimeMillis() - t)  + "ms");
					}
				}
			}
			producer.flush();
			for (Future<RecordMetadata> future : kafkaFutures) {
				future.get();
			}
			long ns = (System.nanoTime() - n);
			LOG.info("Total package [" + c  +"] logs ["+ avc + "] size ["+size+"] in " + String.format("%3.3f", ns/1000000.0)
					+ " ms, avg:" + ns/c + " ns/pack, max pack:" + maxPack + " max count:" + maxC + " max log:" + maxAvroLen );
			producer.close();
			in.close();
		}catch(Exception e) {
			LOG.info("Send " + c + " messages, size " + size);
			LOG.error(e.getMessage(), e);
		}
	}

	private byte[] readNextMessage(InputStream in) throws Exception {
		byte[] message = new byte[1024*1024*300];
		int offset = 0;
		int rb = in.read(message,offset,4);
		int count = byte2Int(message,offset);
		offset += rb;
		for(int i=0;i<count;i++) {
			rb = in.read(message,offset,4);
			int len = byte2Int(message,offset);
			offset += rb;
			rb = in.read(message,offset,len);
			offset += rb;
		}
		byte[] result = new byte[offset];
		System.arraycopy(message, 0, result, 0, offset);
		in.skip(1);
		return result;
	}

	private static int byte2Int(byte[] bs, int offset) {
		return (bs[offset]&0xFF) | ((bs[offset+1] & 0xFF)<<8) | ((bs[offset+2] & 0xFF)<<16) | ((bs[offset+3] & 0xFF)<<24);
	}

	private int checkValid(byte[] bt) {
		try {
			int offset = 0;
			int count = byte2Int(bt,offset);
			offset += 4;
			for(int i=0;i<count;i++) {
				int len = byte2Int(bt,offset);
				offset += 4 + len;
				if (len>maxAvroLen)
					maxAvroLen = len;
			}
			return (offset == bt.length)?count:-1;
		}catch(Exception e) {
			LOG.error(e.getMessage(), e);
			return -1;
		}
	}
}