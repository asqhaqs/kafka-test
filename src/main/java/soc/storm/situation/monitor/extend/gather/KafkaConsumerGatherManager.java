
package soc.storm.situation.monitor.extend.gather;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import backtype.storm.spout.SpoutOutputCollector;

// import org.apache.commons.codec.digest.DigestUtils;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaConsumerGatherManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerGatherManager.class);

    private final KafkaConsumer<String, Object> consumer;

    private final String topic;

    // add zhongsanmu 20171107
    private EnrichmentTask enrichmentTask;
    private ExecutorService enrichmentTaskThreadPool;
    private static int ENRICHMENT_TASK_THREAD_TIMES = Integer.parseInt(SystemConstants.ENRICHMENT_TASK_THREAD_TIMES);

    public KafkaConsumerGatherManager(String topic, String topicMethod, Method getSkyeyeWebFlowLogObjectMethod, String topicNameOutput) {
        logger.info("init KafkaConsumerManager [{}]", topic);
        this.topic = topic;
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());

        // 初始化EnrichmentTask对象 add zhongsanmu 20171107
        this.enrichmentTask = new EnrichmentTask(topic, topicNameOutput);
        this.enrichmentTaskThreadPool = Executors.newFixedThreadPool(ENRICHMENT_TASK_THREAD_TIMES);
        for (int i = 0; i < ENRICHMENT_TASK_THREAD_TIMES; i++) {
            this.enrichmentTaskThreadPool.execute(enrichmentTask);
        }
    }

    private static Properties createConsumerConfig() {
        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "102");
        properties.put("enable.auto.commit", "false");
        // properties.put("enable.auto.commit", "true");
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
    public long execute(SpoutOutputCollector collector) {
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

                // old
                // collector.emit(new Values(consumerRecord.value()));

                //
                enrichmentTask.getMessageCacheProcess().addMessage((byte[]) consumerRecord.value());
            }
        } catch (Exception e) {
            logger.warn("KafkaConsumerTask error", e);
        } finally {
            // consumer.close();
        }

        System.out.println("-------------------------------------------------------------sumCount:" + sumCount);

        return sumCount;
    }

    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        consumer.wakeup();
    }

}
