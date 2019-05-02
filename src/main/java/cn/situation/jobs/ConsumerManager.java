package cn.situation.jobs;

import cn.situation.service.IMessageHandler;
import cn.situation.util.LogUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerManager {

    private static final Logger LOG = LogUtil.getInstance(ConsumerManager.class);
    private static final String PROPERTY_SEPARATOR = ".";

    @Value("${kafka.consumer.source.topic.list}")
    private String kafkaTopicList;
    @Value("${kafka.consumer.group.name}")
    private String consumerGroupName;
    @Value("${application.name}")
    private String consumerInstanceName;
    @Value("${kafka.consumer.brokers.list:localhost:9092}")
    private String kafkaBrokersList;
    @Value("${kafka.consumer.session.timeout.ms:10000}")
    private int consumerSessionTimeoutMs;
    @Value("${kafka.consumer.poll.interval.ms:10000}")
    private long kafkaPollIntervalMs;
    @Value("${kafka.consumer.max.partition.fetch.bytes:1048576}")
    private int maxPartitionFetchBytes;
    @Value("${kafka.consumer.pool.count.list}")
    private String kafkaConsumerPoolCountList;

    @Value("${elasticsearch.index.name.list}")
    private String indexNameList;
    @Value("${elasticsearch.index.type.list}")
    private String indexTypeList;

    @Autowired
    @Qualifier("messageHandler")
    private ObjectFactory<IMessageHandler> messageHandlerObjectFactory;

    @Resource(name = "applicationProperties")
    private Properties applicationProperties;

    @Value("${kafka.consumer.property.prefix:consumer.kafka.property.}")
    private String consumerKafkaPropertyPrefix;

    private List<ExecutorService> consumersThreadPools = new ArrayList<>();
    private List<ConsumerWorker> consumers = new ArrayList<>();
    private Properties kafkaProperties;

    private AtomicBoolean running = new AtomicBoolean(false);

    public ConsumerManager() {
    }

	private void init() {
        LOG.info("init() is starting...");
        kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokersList);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
        kafkaProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumerKafkaPropertyPrefix = consumerKafkaPropertyPrefix.endsWith(PROPERTY_SEPARATOR) ?
                consumerKafkaPropertyPrefix : consumerKafkaPropertyPrefix + PROPERTY_SEPARATOR;
        extractAndSetKafkaProperties(applicationProperties, kafkaProperties, consumerKafkaPropertyPrefix);
        initConsumers();
        LOG.info("init() is finished");
    }

    private void initConsumers() {
        LOG.info("initConsumers() starting...");
        consumers = new ArrayList<>();
        String[] kafkaTopics = kafkaTopicList.split(",");
        String[] kafkaConsumerPoolCounts = kafkaConsumerPoolCountList.split(",");
        String[] indexNames = indexNameList.split(",");
        String[] indexTypes = indexTypeList.split(",");
        for (int i = 0; i < kafkaTopics.length; i++) {
            String kafkaTopic = kafkaTopics[i];
            int consumerPoolCount = Integer.parseInt(kafkaConsumerPoolCounts[i]);
            String indexName = indexNames[i];
            String indexType = indexTypes[i];
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(kafkaTopic + "-thread-%d").build();
            ExecutorService consumersThreadPool = Executors.newFixedThreadPool(consumerPoolCount, threadFactory);
            for (int consumerNumber = 0; consumerNumber < consumerPoolCount; consumerNumber++) {
                ConsumerWorker consumer = new ConsumerWorker(
                        consumerInstanceName + "-" + kafkaTopic + "-" + consumerNumber,
                        kafkaTopic, kafkaProperties, kafkaPollIntervalMs,
                        messageHandlerObjectFactory.getObject(), indexName, indexType);
                consumers.add(consumer);
                consumersThreadPool.submit(consumer);
            }
            consumersThreadPools.add(consumersThreadPool);
        }
        LOG.info("initConsumers() finished");
    }

    private void shutdownConsumers() {
        LOG.info("shutdownConsumers() starting...");
        if (consumers != null) {
            for (ConsumerWorker consumer : consumers) {
                consumer.shutdown();
            }
        }
        if (!consumersThreadPools.isEmpty()) {
            consumersThreadPools.forEach(consumersThreadPool -> {
                if (consumersThreadPool != null) {
                    consumersThreadPool.shutdown();
                    try {
                        consumersThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            });
        }
        if (consumers != null) {
            consumers.forEach(consumer -> consumer.getPartitionOffsetMap()
                    .forEach((topicPartition, offset)
                            -> LOG.info(String.format("[%s]: consumerId<%s>, partition<%s>, offset<%s>",
                            "shutdownConsumers", consumer.getConsumerId(), topicPartition.partition(), offset.offset()))));
        }
        LOG.info("shutdownConsumers() finished");
    }

    @PostConstruct
    public void postConstruct() {
        start();
    }

    @PreDestroy
    public void preDestroy() {
        stop();
    }

    synchronized private void start() {
        if (!running.getAndSet(true)) {
            init();
        } else {
            LOG.warn("Already running...");
        }
    }

    synchronized private void stop() {
        if (running.getAndSet(false)) {
            shutdownConsumers();
        } else {
            LOG.warn("Already stopped");
        }
    }

    private static void extractAndSetKafkaProperties(Properties applicationProperties,
                                                    Properties kafkaProperties,
                                                     String kafkaPropertyPrefix) {
        if(applicationProperties != null && applicationProperties.size() >0) {
            for(Map.Entry <Object,Object> currentPropertyEntry :applicationProperties.entrySet()) {
                String propertyName = currentPropertyEntry.getKey().toString();
                if(StringUtils.isNotBlank(propertyName) && propertyName.contains(kafkaPropertyPrefix)) {
                    String validKafkaConsumerProperty = propertyName.replace(kafkaPropertyPrefix, StringUtils.EMPTY);
                    kafkaProperties.put(validKafkaConsumerProperty, currentPropertyEntry.getValue());
                    LOG.info(String.format("[%s]: prefix<%s>, key<%s>, value<%s>", "extractAndSetKafkaProperties",
                            kafkaPropertyPrefix, validKafkaConsumerProperty, currentPropertyEntry.getValue()));
                }
            }
        }
    }
}