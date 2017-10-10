
package soc.storm.situation.monitor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
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

    private static Properties kafkaProducerProperties = new Properties();
    private String topic;// = "ty_tcpflow_output";

    private String topicProperties;// = producer.getTopicProperties(topic);
    private OutputCollector outputCollector;
    private static KafkaProducer<String, byte[]> producer = null;

    static {
        try {
            logger.info("init kafkaProducerProperties");

            kafkaProducerProperties.put("bootstrap.servers", SystemConstants.BROKER_URL);
            kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProducerProperties.put("batch.size", 1);// 16384
            kafkaProducerProperties.put("linger.ms", 1);
            kafkaProducerProperties.put("buffer.memory", 33554432);
            kafkaProducerProperties.put("acks", "0");
            kafkaProducerProperties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy。
            kafkaProducerProperties.put("topic.properties.fetch.enable", "true");

            producer = new KafkaProducer<String, byte[]>(kafkaProducerProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public KafkaProcuderBolt(String topicNameOutput) {
        topic = topicNameOutput;
        topicProperties = producer.getTopicProperties(topic);
        // topicProperties =
        // "{\"name\":\"ty_dns\",\"namespace\":\"enrichment_ip\",\"type\":\"record\",\"fields\":[{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"string\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"string\",\"null\"]},{\"name\":\"dns_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"host\",\"type\":[\"string\",\"null\"]},{\"name\":\"host_md5\",\"type\":[\"string\",\"null\"]},{\"name\":\"addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"mx\",\"type\":[\"string\",\"null\"]},{\"name\":\"cname\",\"type\":[\"string\",\"null\"]},{\"name\":\"reply_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"count\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]}]}";
        // topicProperties =
        // "{\"name\":\"ty_dns\",\"namespace\":\"enrichment_ip\",\"type\":\"record\",\"fields\":[{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dns_type\",\"type\":[\"int\",\"null\"]},{\"name\":\"host\",\"type\":[\"string\",\"null\"]},{\"name\":\"host_md5\",\"type\":[\"string\",\"null\"]},{\"name\":\"addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"mx\",\"type\":[\"string\",\"null\"]},{\"name\":\"cname\",\"type\":[\"string\",\"null\"]},{\"name\":\"reply_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"count\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]}]}";
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    public void execute(Tuple tuple) {
        Map<String, Object> skyeyeWebFlowLogMap = (Map<String, Object>) tuple.getValue(0);
        System.out.println("--------------------[" + topic + "] skyeyeWebFlowLogMap: " + JsonUtils.mapToJson(skyeyeWebFlowLogMap));
        try {
            // String topicProperties = producer.getTopicProperties(topic);

            // Schema.Parser parser = new Schema.Parser();
            // Schema topicSchema = parser.parse(KafkaProcuderBolt.class.getResourceAsStream("/avro/tcp_flowaa.avsc"));

            Schema topicSchema = new Schema.Parser().parse(topicProperties);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(topicSchema);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            if (null != skyeyeWebFlowLogMap && 0 != skyeyeWebFlowLogMap.size()) {
                GenericRecord record = new GenericData.Record(topicSchema);
                for (Map.Entry<String, Object> entry : skyeyeWebFlowLogMap.entrySet()) {
                    record.put(entry.getKey(), entry.getValue());
                }
                datumWriter.write(record, encoder);
            }
            encoder.flush();
            byte[] sendData = out.toByteArray();
            producer.send(new ProducerRecord<String, byte[]>(topic, null, sendData));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 更新kafka中partitionManager对应的offset
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

}
