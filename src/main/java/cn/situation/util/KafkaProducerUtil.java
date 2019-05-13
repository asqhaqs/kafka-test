package cn.situation.util;

import cn.situation.cons.SystemConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;

import java.io.File;
import java.util.Properties;

/**
 * @author lenzhao
 * @date 2019/5/13 15:59
 * @description TODO
 */
public class KafkaProducerUtil {

    private static final Logger LOG = LogUtil.getInstance(KafkaProducerUtil.class);
    private static KafkaProducer<String, byte[]> producer;

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
        kafkaProducerProperties.put("batch.size", 1638400);
        kafkaProducerProperties.put("linger.ms", 5000);
        kafkaProducerProperties.put("buffer.memory", 33554432);
        kafkaProducerProperties.put("acks", "0");
        if (SystemConstant.IS_KERBEROS.equals("true")) {
            kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
        }
        return kafkaProducerProperties;
    }

    public static KafkaProducer<String, byte[]> getInstance() {
        return producer;
    }
}
