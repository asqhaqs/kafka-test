
package soc.storm.situation.monitor.extend;

import java.util.Arrays;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;

@Deprecated
public class KafkaConsumerTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTask.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;
    private Queue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>();

    private volatile boolean running = true;
    private Thread runThread;

    public KafkaConsumerTask(String topic) {
        this.consumer = new KafkaConsumer<String, byte[]>(createConsumerConfig());
        this.topic = topic;
    }

    private static Properties createConsumerConfig() {
        logger.info("init createConsumerConfig");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SystemConstants.BROKER_URL);
        properties.put("group.id", SystemConstants.TOPOLOGY_NAME + "1122");
        // properties.put("enable.auto.commit", "false");
        properties.put("enable.auto.commit", "true");
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
    public void run() {
        runThread = Thread.currentThread();
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (running && !runThread.isInterrupted()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                consumer.commitSync();
                for (ConsumerRecord<String, byte[]> consumerRecord : records) {
                    // System.out.println(String.format("--------------------offset=%d,key=%s,value=%s",
                    // consumerRecord.offset(),
                    // consumerRecord.key(),
                    // consumerRecord.value()));

                    queue.add(consumerRecord.value());
                }

                // Utils.sleep(500);
            }
        } catch (Exception e) {
            logger.warn("KafkaConsumerTask error", e);
        }

    }

    public Queue<byte[]> getQueue() {
        return queue;
    }

    /**
     * 线程停止
     */
    public void closeKafkaConsumerTask() {
        logger.warn("closeKafkaConsumerTask");
        running = false;
        runThread.interrupt();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerTask consumerThread = new KafkaConsumerTask("ty_tcpflow");
        consumerThread.start();
        // Thread.sleep(10000);
        // consumerThread.interrupt();
    }
}
