
package soc.storm.situation.monitor.extend.compresstdgywa3061;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.utils.JsonUtils;
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

    static {
        System.out.println("--------------------KafkaProcuderBolt-------------SystemConstants.BROKER_URL:" + SystemConstants.BROKER_URL);
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
                SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }

    private static Properties kafkaProducerProperties = new Properties();
    private String topic;// = "ty_tcpflow_output";

    private String topicProperties;// = producer.getTopicProperties(topic);

    // avro schema: record 嵌套 record
    private static Map<String, String> recordArrayRecordTopicMap = new HashMap<String, String>();
    private static Map<String, String> recordArrayRecordMap = new HashMap<String, String>();
    private boolean isRecordArrayRecordTopic;

    // private OutputCollector outputCollector;
    private static KafkaProducer<String, byte[]> producer = null;

    static {
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

            producer = new KafkaProducer<String, byte[]>(kafkaProducerProperties);

            try {
                // FileUtil.testConfigFile("KafkaProcuderBolt");
            } catch (Exception e1) {
                e1.printStackTrace();
            }// --add zhongsanmu 20180104
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public KafkaProcuderBolt(String topicNameOutput) {
        topic = topicNameOutput;
        //
        isRecordArrayRecordTopic = recordArrayRecordTopicMap.containsKey(topic);
        topicProperties = producer.getTopicProperties(topic);
        System.out.println("--------------------topic:" + topic + "topicProperties:" + topicProperties);
        // topicProperties =
        // "{\"name\":\"ty_dns\",\"namespace\":\"enrichment_ip\",\"type\":\"record\",\"fields\":[{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"string\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"string\",\"null\"]},{\"name\":\"dns_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"host\",\"type\":[\"string\",\"null\"]},{\"name\":\"host_md5\",\"type\":[\"string\",\"null\"]},{\"name\":\"addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"mx\",\"type\":[\"string\",\"null\"]},{\"name\":\"cname\",\"type\":[\"string\",\"null\"]},{\"name\":\"reply_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"count\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]}]}";
        // topicProperties =
        // "{\"name\":\"ty_dns\",\"namespace\":\"enrichment_ip\",\"type\":\"record\",\"fields\":[{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dns_type\",\"type\":[\"int\",\"null\"]},{\"name\":\"host\",\"type\":[\"string\",\"null\"]},{\"name\":\"host_md5\",\"type\":[\"string\",\"null\"]},{\"name\":\"addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"mx\",\"type\":[\"string\",\"null\"]},{\"name\":\"cname\",\"type\":[\"string\",\"null\"]},{\"name\":\"reply_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"count\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]}]}";
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // this.outputCollector = collector;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple tuple) {
        long begin = System.currentTimeMillis();
        // Map<String, Object> skyeyeWebFlowLogMap = (Map<String, Object>) tuple.getValue(0);
        List<Map<String, Object>> skyeyeWebFlowLogMapList = (List<Map<String, Object>>) tuple.getValue(0);

        try {
            long avroCompressBegin = System.currentTimeMillis();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            for (Map<String, Object> skyeyeWebFlowLogMap : skyeyeWebFlowLogMapList) {
                //
                if (null != skyeyeWebFlowLogMap && 0 != skyeyeWebFlowLogMap.size()) {
                    // String topicProperties = producer.getTopicProperties(topic);

                    // Schema.Parser parser = new Schema.Parser();
                    // Schema topicSchema =
                    // parser.parse(KafkaProcuderBolt.class.getResourceAsStream("/avro/tcp_flowaa.avsc"));

                    System.out.println("--------------------[" + topic + "] KafkaProcuderBolt skyeyeWebFlowLogMap: "
                            + JsonUtils.mapToJson(skyeyeWebFlowLogMap));

                    Schema topicSchema = new Schema.Parser().parse(topicProperties);
                    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(topicSchema);

                    GenericRecord record = new GenericData.Record(topicSchema);

                    try {
                        for (Map.Entry<String, Object> entry : skyeyeWebFlowLogMap.entrySet()) {
                            // record.put(entry.getKey(), entry.getValue());

                            if (isRecordArrayRecordTopic && recordArrayRecordMap.containsKey(topic + ":" + entry.getKey())) {
                                Schema subSchemaElementType = null;
                                List<GenericRecord> subRecordArray = new ArrayList<GenericRecord>();

                                List<Map<String, Object>> subRecordMapList = (List<Map<String, Object>>) entry.getValue();
                                if (subRecordMapList != null && subRecordMapList.size() > 0) {
                                    subSchemaElementType = KafkaProcuderBolt.getSubSchemaElementType(topicSchema, entry.getKey());
                                }
                                for (Map<String, Object> subRecordMap : subRecordMapList) {
                                    GenericRecord subRecord = new GenericData.Record(subSchemaElementType);
                                    for (Map.Entry<String, Object> subRecordEntry : subRecordMap.entrySet()) {
                                        subRecord.put(subRecordEntry.getKey(), subRecordEntry.getValue());
                                    }

                                    subRecordArray.add(subRecord);
                                }

                                record.put(entry.getKey(), subRecordArray);
                            } else {
                                record.put(entry.getKey(), entry.getValue());
                            }
                        }

                        datumWriter.write(record, encoder);
                    } catch (Exception e) {
                        // e.printStackTrace();
                        System.out.println("--------------------[" + topic + "] skyeyeWebFlowLogMap: " + JsonUtils
                                .mapToJson(skyeyeWebFlowLogMap) + ",  " + e.getMessage());
                    }
                }
            }

            // Schema topicSchema = new Schema.Parser().parse(topicProperties);
            // DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(topicSchema);
            //
            // if (null != skyeyeWebFlowLogMap && 0 != skyeyeWebFlowLogMap.size()) {
            // GenericRecord record = new GenericData.Record(topicSchema);
            // for (Map.Entry<String, Object> entry : skyeyeWebFlowLogMap.entrySet()) {
            // record.put(entry.getKey(), entry.getValue());
            // }
            // datumWriter.write(record, encoder);
            // }

            encoder.flush();
            out.flush();
            long avroCompressEnd = System.currentTimeMillis();
            // System.out.println("###########################KafkaProcuderBolt, avroCompress use time: "
            // + (avroCompressEnd - avroCompressBegin) + "ms, skyeyeWebFlowLogMapList.size():" +
            // skyeyeWebFlowLogMapList.size());

            long compressBegin = System.currentTimeMillis();
            byte[] sendData = out.toByteArray();
            // TODO:
            // byte[] sendData = SnappyCompress.compress(out.toByteArray());

            // System.out.println("----------------------------out.toByteArray().length:" + out.toByteArray().length);
            // byte[] sendData = SnappyCompress.compress001(out.toByteArray());
            // System.out.println("----------------------------SnappyCompress.compress001(out.toByteArray()).length:" +
            // SnappyCompress.compress001(out.toByteArray()).length);
            long compressEnd = System.currentTimeMillis();
            // System.out.println("###########################KafkaProcuderBolt, compress use time: "
            // + (compressEnd - compressBegin) + "ms");

            long sendBegin = System.currentTimeMillis();
            // TODO: test
            producer.send(new ProducerRecord<String, byte[]>(topic, null, sendData));
            long sendEnd = System.currentTimeMillis();
            // System.out.println("###########################KafkaProcuderBolt, send use time: "
            // + (sendEnd - sendBegin) + "ms");

            // producer.send(new ProducerRecord<String, byte[]>(topic, null, "sendData".getBytes()));
            // System.out.println("------------KafkaProcuderBolt---topic:" + topic + "--------sendData.length:" +
            // sendData.length);
            // ------------KafkaProcuderBolt---topic:ty_tcpflow_outputtest1--------sendData.length:1646000
            // ------------KafkaProcuderBolt---topic:ty_tcpflow_outputtest1--------sendData.length:94412

            long end = System.currentTimeMillis();
            System.out
                    .println("#################################################################################KafkaProcuderBolt, use time: "
                            + (end - begin) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("topic:" + topic + "," + e.getMessage());
        }

        // delete zhongsanmu 20171031
        // 更新kafka中partitionManager对应的offset
        // this.outputCollector.ack(tuple);
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
