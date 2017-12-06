
package soc.storm.situation.test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaProducerConnectTest {

    private static Properties kafkaProducerProperties = new Properties();
    private static String topic = "td_tcpflow";

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        kafkaProducerProperties.put("bootstrap.servers", "172.24.2.155:9092");
        kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put("batch.size", 1);
        kafkaProducerProperties.put("linger.ms", 1);
        kafkaProducerProperties.put("buffer.memory", 33554432);// 32M
        kafkaProducerProperties.put("acks", "0");
        kafkaProducerProperties.put("compression.type", "none");// #消息压缩模式，默认是none，可选gzip、snappy。
        // kafkaProducerProperties.put("topic.properties.fetch.enable", "true");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProperties);
        try {
            String sendData = "test send data";
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic, null, sendData));
            try {
                future.get(4, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        } finally {
            producer.close();
        }

    }

}
