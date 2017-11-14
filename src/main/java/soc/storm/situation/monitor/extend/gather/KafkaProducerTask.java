
package soc.storm.situation.monitor.extend.gather;

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

/**
 * KafkaProcuderBolt
 *
 * @author zhongsanmu
 *
 */
public class KafkaProducerTask implements Runnable {// extends BaseRichBolt

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTask.class);

    private static Properties kafkaProducerProperties = new Properties();
    private String topicNameOutput;// = "ty_tcpflow_output";

    private String topicProperties = null;// = producer.getTopicProperties(topic);
    private static KafkaProducer<String, byte[]> producer = null;
    // private static int executeInitCount = 0;
    // private static int executeStaticCount = 0;

    // add zhongsanmu 20171107
    private MessageCacheProcess<Map<String, Object>> messageCacheProcess = new MessageCacheProcess<Map<String, Object>>();

    static {
        try {
            logger.info("init kafkaProducerProperties");

            kafkaProducerProperties.put("bootstrap.servers", SystemConstants.BROKER_URL);
            kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            // kafkaProducerProperties.put("batch.size", 1);// 16384
            kafkaProducerProperties.put("batch.size", 16384);// 16384
            kafkaProducerProperties.put("linger.ms", 1);
            kafkaProducerProperties.put("buffer.memory", 33554432);
            kafkaProducerProperties.put("acks", "0");
            kafkaProducerProperties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy。
            kafkaProducerProperties.put("topic.properties.fetch.enable", "true");

            producer = new KafkaProducer<String, byte[]>(kafkaProducerProperties);

            // executeStaticCount++;
            // System.out.println("------------------------------------------------------executeStaticCount:" +
            // executeStaticCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MessageCacheProcess<Map<String, Object>> getMessageCacheProcess() {
        return messageCacheProcess;
    }

    public KafkaProducerTask(String topicNameOutput) {
        synchronized (this.getClass()) {
            // executeInitCount++;
            // System.out.println("---------executeInitCount:" + executeInitCount
            // + "-----this:" + this.toString()
            // + "-----topicNameOutput:" + topicNameOutput);

            this.topicNameOutput = topicNameOutput;
            if (null == this.topicProperties) {
                this.topicProperties = producer.getTopicProperties(topicNameOutput);
            }
        }

        // this.topicProperties =
        // "{\"type\":\"record\",\"name\":\"ty_tcpflow_output22_schema\",\"namespace\":\"ty_tcpflow_output22_schema\",\"fields\":[{\"name\":\"summary\",\"type\":[\"string\",\"null\"]},{\"name\":\"src_mac\",\"type\":[\"string\",\"null\"]},{\"name\":\"dtime\",\"type\":[\"string\",\"null\"]},{\"name\":\"up_payload\",\"type\":[\"string\",\"null\"]},{\"name\":\"uplink_length\",\"type\":[\"string\",\"null\"]},{\"name\":\"stime\",\"type\":[\"string\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"dst_mac\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"string\",\"null\"]},{\"name\":\"downlink_length\",\"type\":[\"string\",\"null\"]},{\"name\":\"down_payload\",\"type\":[\"string\",\"null\"]},{\"name\":\"proto\",\"type\":[\"string\",\"null\"]},{\"name\":\"client_os\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"server_os\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"string\",\"null\"]},{\"name\":\"status\",\"type\":[\"string\",\"null\"]}]}";
        // topicProperties =
        // "{\"name\":\"ty_dns\",\"namespace\":\"enrichment_ip\",\"type\":\"record\",\"fields\":[{\"name\":\"serial_num\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"sport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dip\",\"type\":[\"string\",\"null\"]},{\"name\":\"dport\",\"type\":[\"int\",\"null\"]},{\"name\":\"dns_type\",\"type\":[\"int\",\"null\"]},{\"name\":\"host\",\"type\":[\"string\",\"null\"]},{\"name\":\"host_md5\",\"type\":[\"string\",\"null\"]},{\"name\":\"addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"mx\",\"type\":[\"string\",\"null\"]},{\"name\":\"cname\",\"type\":[\"string\",\"null\"]},{\"name\":\"reply_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"count\",\"type\":[\"string\",\"null\"]},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]}]}";
    }

    /**
     * 
     * @param skyeyeWebFlowLogMap
     */
    private void kafkaProducer(Map<String, Object> skyeyeWebFlowLogMap) {
        //
        if (null == skyeyeWebFlowLogMap) {
            return;
        }

        try {
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
            producer.send(new ProducerRecord<String, byte[]>(topicNameOutput, null, sendData));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            // while (running && !runThread.isInterrupted()) {
            while (true) {
                //
                Map<String, Object> skyeyeWebFlowLogMap = messageCacheProcess.getMessage();

                //
                kafkaProducer(skyeyeWebFlowLogMap);
            }
        } catch (Exception e) {
        } finally {
        }
    }

}
