
package soc.storm.situation.test.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import soc.storm.situation.contants.SystemConstants;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaProducerTest {

    private static Properties kafkaProducerProperties = new Properties();
    private static String topic = "skyeye_weblog";// skyeye_ssl test_kerberos test_001 test_kerberos skyeye_weblog

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // System.setProperty("java.security.auth.login.config", "D:\\kerberos_config\\kafka_server_jaas.conf");
        // System.setProperty("java.security.krb5.conf", "D:\\kerberos_config\\krb5.conf");

        // zookeeper_hosts=10.187.208.11:2181,10.187.208.12:2181,10.187.208.13:2181
        // broker_url=10.187.208.12:9092,10.187.208.14:9092,10.187.208.15:9092
        // kafkaProducerProperties.put("bootstrap.servers", "172.24.2.155:9092");
        // kafkaProducerProperties.put("bootstrap.servers", "10.187.208.12:9092");
        kafkaProducerProperties.put("bootstrap.servers", "10.95.26.29:9092");
        // kafkaProducerProperties.put("bootstrap.servers", "bg04.situation.360es.net:9092");
        kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProperties.put("batch.size", 1);
        kafkaProducerProperties.put("linger.ms", 1);
        kafkaProducerProperties.put("buffer.memory", 33554432);// 32M
        kafkaProducerProperties.put("acks", "0");
        kafkaProducerProperties.put("compression.type", "none");// #消息压缩模式，默认是none，可选gzip、snappy。
        kafkaProducerProperties.put("topic.properties.fetch.enable", "true");
        // kerberos 授权&认证
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
        }

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProperties);

        String topicProperties = producer.getTopicProperties(topic);
        System.out.println("--------------------topic:" + topic + "topicProperties:" + topicProperties);

        String sendDataStr = "zhongsanmu201801261401";
        // KafkaConsumerTest consumerTest = new KafkaConsumerTest("skyeye_ssl");// skyeye_ssl
        // byte[] sendData = consumerTest.getRecordValue();
        // byte[] sendData = sendDataStr.getBytes();
        // System.out.println("-----------sendData.length:" + sendData.length);
        System.out.println("-----------sendData.length:" + sendDataStr.length());

        try {
            for (int i = 0; i < 1; i++) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic, null, sendDataStr));
                try {
                    future.get(4, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }

            Thread.sleep(3000);
        } finally {
            producer.close();
        }

    }
}
