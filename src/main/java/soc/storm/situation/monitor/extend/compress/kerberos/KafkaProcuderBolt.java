
package soc.storm.situation.monitor.extend.compress.kerberos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.utils.FileUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * KafkaProcuderBolt
 *
 * @author zhongsanmu
 *
 */
public class KafkaProcuderBolt extends BaseRichBolt {

    /**
     *
     */
    private static final long serialVersionUID = -2639126860311224615L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaProcuderBolt.class);

    // static {
    // System.out.println("--------------------KafkaProcuderBolt-------------SystemConstants.BROKER_URL:" +
    // SystemConstants.BROKER_URL);
    // if (SystemConstants.IS_KERBEROS.equals("true")) {
    // System.setProperty("java.security.auth.login.config",
    // SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
    // System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator +
    // "krb5.conf");
    // }
    // }

    private static Properties kafkaProducerProperties = new Properties();
    private String topic;// = "ty_tcpflow_output";

    // avro schema: record 嵌套 record
    private static Map<String, String> recordArrayRecordTopicMap = new HashMap<String, String>();
    private static Map<String, String> recordArrayRecordMap = new HashMap<String, String>();

    private static KafkaProducer<String, Object> producer = null;

    static {
        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");
        try {
            FileUtil.testConfigFile("KafkaProcuderBolt");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        // （1）初始化 recordArrayRecordMap
        String[] recordArrayRecordArray = SystemConstants.RECORD_ARRAY_RECORD.split(",");
        for (String recordArrayRecordStr : recordArrayRecordArray) {
            String[] recordArrayRecordSubArray = recordArrayRecordStr.split(":");
            recordArrayRecordTopicMap.put(recordArrayRecordSubArray[0], null);
            recordArrayRecordMap.put(recordArrayRecordStr, null);
        }

        // （2）初始化 KafkaProducer
        try {
            logger.info("init kafkaProducerProperties");
            kafkaProducerProperties.put("bootstrap.servers", SystemConstants.BROKER_URL);
            kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            // kafkaProducerProperties.put("batch.size", 16384);// 16384
            kafkaProducerProperties.put("batch.size", 1);// 16384
            kafkaProducerProperties.put("linger.ms", 1);
            kafkaProducerProperties.put("buffer.memory", 33554432);
            kafkaProducerProperties.put("acks", "0");
            kafkaProducerProperties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy、lz4。
            kafkaProducerProperties.put("topic.properties.fetch.enable", "true");
            // kerberos 授权&认证
            if (SystemConstants.IS_KERBEROS.equals("true")) {
                kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
                kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
            }

            producer = new KafkaProducer<String, Object>(kafkaProducerProperties);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public KafkaProcuderBolt(String topicNameOutput) {
        topic = topicNameOutput;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");
        try {
            FileUtil.testConfigFile("KafkaProcuderBolt");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104
    }

    @Override
    public void execute(Tuple tuple) {
        Object skyeyeWebFlowLogMapList = tuple.getValue(0);

        try {
            producer.send(new ProducerRecord<String, Object>(topic, null, skyeyeWebFlowLogMapList));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("topic:" + topic + "," + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * 
     * @param topicSchema
     * @return
     */
    public static Schema getSubSchemaElementType(Schema topicSchema, String subName) {
        Schema subSchema = null;
        Schema.Field subField = topicSchema.getField(subName);
        List<Schema> schemaList = subField.schema().getTypes();
        for (Schema schema : schemaList) {
            if (schema.getType().equals(Type.ARRAY)) {
                subSchema = schema;
            }
        }
        return subSchema.getElementType();
    }
}
