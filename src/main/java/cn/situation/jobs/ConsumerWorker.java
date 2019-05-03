package cn.situation.jobs;

import cn.situation.cons.SystemConstant;
import cn.situation.service.IMessageHandler;
import cn.situation.service.OffsetLoggingCallbackImpl;
import cn.situation.util.LogUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.util.*;

public class ConsumerWorker implements Runnable {

	private static final Logger LOG = LogUtil.getInstance(ConsumerWorker.class);
	private IMessageHandler messageHandler;
	private final KafkaConsumer<String, String> consumer;
	private final String kafkaTopic;
	private final String consumerId;
	private long pollIntervalMs;
	private OffsetLoggingCallbackImpl offsetLoggingCallback;
	private String indexName;
	private String indexType;
	private String redisKey;
	private String outStrategy;

	public ConsumerWorker(String consumerId,String kafkaTopic, Properties kafkaProperties,
			long pollIntervalMs, IMessageHandler messageHandler, String indexName, String indexType,
						  String redisKey, String outStrategy) {
		this.messageHandler = messageHandler;
		kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		this.consumerId = consumerId;
		this.kafkaTopic = kafkaTopic;
		this.pollIntervalMs = pollIntervalMs;
		this.indexName = indexName;
		this.indexType = indexType;
		this.redisKey = redisKey;
		this.outStrategy = outStrategy;
		consumer = new KafkaConsumer<>(kafkaProperties);
		offsetLoggingCallback = new OffsetLoggingCallbackImpl();
		LOG.info(String.format("[%s]: consumerId<%s>, kafkaTopic<%s>, kafkaProperties<%s>, indexName<%s>, indexType<%s>",
				"ConsumerWorker", consumerId, kafkaTopic, kafkaProperties, indexName, indexType));
	}

	@Override
	public void run() {
		try {
			LOG.info(String.format("[%s]: consumerId<%s>, message<%s>", "run", consumerId, "Starting ConsumerWorker"));
			consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
			while (true) {
				boolean isPollFirstRecord = true;
				int numProcessedMessages = 0;
				int numSkippedIndexingMessages = 0;
				int numMessagesInBatch = 0;
				long pollStartMillis = 0L;
				ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);
				Map<Integer, Long> partitionOffsetMap = new HashMap<>();
				List<String> msgList = new ArrayList<>();
				for (ConsumerRecord<String, String> record : records) {
					numMessagesInBatch++;
					LOG.info(String.format("[%s]: consumerId<%s>, partition<%s>, offset<%s>, value<%s>",
							"run", consumerId, record.partition(), record.offset(), record.value()));
					if (isPollFirstRecord) {
						isPollFirstRecord = false;
						pollStartMillis = System.currentTimeMillis();
					}
					try {
						String processedMessage = messageHandler.transformMessage(record.value(), record.offset());
						msgList.add(processedMessage);
						partitionOffsetMap.put(record.partition(), record.offset());
						numProcessedMessages++;
					} catch (Exception e) {
						numSkippedIndexingMessages++;
						LOG.error(e.getMessage(), e);
					}
				}
				long timeBeforePost = System.currentTimeMillis();
				boolean moveToNextBatch = false;
				if (!records.isEmpty()) {
					moveToNextBatch = postData(msgList);
					long timeToPost = System.currentTimeMillis();
					double perMessageTimeMillis = (double) (timeToPost - pollStartMillis) / numProcessedMessages;
					LOG.debug(String.format("[%s]: totalMessage<%s>, messageProcessed<%s>, messageSkipped<%s>, " +
							"time2CreateBatch<%s>, time2PostMs<%s>, perMessageTimeMs<%s>", "run", numMessagesInBatch,
							numProcessedMessages, numSkippedIndexingMessages, timeBeforePost - pollStartMillis,
							timeToPost - pollStartMillis, perMessageTimeMillis));
				}
				if (moveToNextBatch) {
					LOG.info(String.format("[%s]: partitionOffsetMap<%s>", "run",
							partitionOffsetMap));
					consumer.commitAsync(offsetLoggingCallback);
				}
			}
		} catch (WakeupException e) {
			LOG.warn(String.format("[%s]: consumerId<%s>, WakeupException<%s>", "run", consumerId, e.getMessage()), e);
		}catch (Exception e) {
			LOG.error(String.format("[%s]: consumerId<%s>, Exception<%s>", "run", consumerId, e.getMessage()), e);
		} finally {
			consumer.close();
		}
	}
	
	private boolean postData(List<String> msgList) {
		boolean moveToTheNextBatch = false;
		try {
			if (SystemConstant.ES_STRATEGY.equals(outStrategy)) {
				List<Object> dataList = new ArrayList<>();
				for (String msg : msgList) {
					Map<String, String> data = messageHandler.addMessageToBatch(msg, indexName, indexType);
					dataList.add(data);
				}
				moveToTheNextBatch = messageHandler.postToElasticSearch(dataList);
			} else if (SystemConstant.REDIS_STRATEGY.equals(outStrategy)) {
				moveToTheNextBatch = messageHandler.postToRedis(redisKey, msgList);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return moveToTheNextBatch;
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