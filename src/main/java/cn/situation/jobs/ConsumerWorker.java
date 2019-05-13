package cn.situation.jobs;

import cn.situation.cons.SystemConstant;
import cn.situation.service.OffsetLoggingCallbackImpl;
import cn.situation.util.FileUtil;
import cn.situation.util.KafkaProducerUtil;
import cn.situation.util.LogUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.io.File;
import java.util.*;

public class ConsumerWorker implements Runnable {

	private static final Logger LOG = LogUtil.getInstance(ConsumerWorker.class);
	private final KafkaConsumer<String, byte[]> consumer;
	private final String kafkaTopic;
	private final String consumerId;
	private long pollIntervalMs;
	private OffsetLoggingCallbackImpl offsetLoggingCallback;

	public ConsumerWorker(String consumerId,String kafkaTopic, Properties kafkaProperties, long pollIntervalMs) {
		kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		this.consumerId = consumerId;
		this.kafkaTopic = kafkaTopic;
		this.pollIntervalMs = pollIntervalMs;
		consumer = new KafkaConsumer<>(kafkaProperties);
		offsetLoggingCallback = new OffsetLoggingCallbackImpl();
		LOG.info(String.format("[%s]: consumerId<%s>, kafkaTopic<%s>, kafkaProperties<%s>", "ConsumerWorker",
				consumerId, kafkaTopic, kafkaProperties));
	}

	@Override
	public void run() {
		try {
			LOG.info(String.format("[%s]: consumerId<%s>, message<%s>", "run", consumerId, "Starting ConsumerWorker"));
			consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
			long count = 0;
			String filePath = SystemConstant.GEO_DATA_PATH + File.separator + kafkaTopic + ".txt";
			while (true) {
				boolean isPollFirstRecord = true;
				int numProcessedMessages = 0;
				int numSkippedIndexingMessages = 0;
				int numMessagesInBatch = 0;
				long pollStartMillis = 0L;
				ConsumerRecords<String, byte[]> records = consumer.poll(pollIntervalMs);
				Map<Integer, Long> partitionOffsetMap = new HashMap<>();
				List<byte[]> msgList = new ArrayList<>();
				for (ConsumerRecord<String, byte[]> record : records) {
					numMessagesInBatch++;
					LOG.debug(String.format("[%s]: consumerId<%s>, partition<%s>, offset<%s>, value<%s>",
							"run", consumerId, record.partition(), record.offset(), Arrays.toString(record.value())));
					if (isPollFirstRecord) {
						isPollFirstRecord = false;
						pollStartMillis = System.currentTimeMillis();
					}
					try {
						// FileUtil.writeFile(filePath, record.value(), false);
						msgList.add(record.value());
						partitionOffsetMap.put(record.partition(), record.offset());
						numProcessedMessages++;
					} catch (Exception e) {
						numSkippedIndexingMessages++;
						LOG.error(e.getMessage(), e);
					}
				}
				long timeBeforePost = System.currentTimeMillis();
				if (!records.isEmpty()) {
					// FileUtil.writeFile(SystemConstant.GEO_DATA_PATH, kafkaTopic + ".txt", msgList, true);
					long timeToPost = System.currentTimeMillis();
					double perMessageTimeMillis = (double) (timeToPost - pollStartMillis) / numProcessedMessages;
					LOG.debug(String.format("[%s]: totalMessage<%s>, messageProcessed<%s>, messageSkipped<%s>, " +
							"time2CreateBatch<%s>, time2PostMs<%s>, perMessageTimeMs<%s>", "run", numMessagesInBatch,
							numProcessedMessages, numSkippedIndexingMessages, timeBeforePost - pollStartMillis,
							timeToPost - pollStartMillis, perMessageTimeMillis));
				}
				count = count + numProcessedMessages;
				LOG.info(String.format("[%s]: partitionOffsetMap<%s>", "run", partitionOffsetMap));
				consumer.commitAsync(offsetLoggingCallback);
				LOG.info(String.format("[%s]: topicName<%s>, count<%s>", "run", kafkaTopic, count));
//				for (int i = 0; i < 1000; i++) {
//					for (byte[] msg : msgList) {
//						KafkaProducerUtil.getInstance().send(new ProducerRecord<>(kafkaTopic, null, msg));
//					}
//				}
			}
		} catch (WakeupException e) {
			LOG.warn(String.format("[%s]: consumerId<%s>, WakeupException<%s>", "run", consumerId, e.getMessage()));
		}catch (Exception e) {
			LOG.error(String.format("[%s]: consumerId<%s>, Exception<%s>", "run", consumerId, e.getMessage()), e);
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
		return offsetLoggingCallback.getPartitionOffsetMap();
	}

	public String getConsumerId() {
		return consumerId;
	}
}