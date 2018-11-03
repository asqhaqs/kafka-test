package soc.storm.situation.monitor.extend.gatherdalian;

import java.io.ByteArrayOutputStream;
import java.io.File;
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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import soc.storm.situation.contants.SystemMapEnrichConstants;
import soc.storm.situation.utils.JsonUtils;

/**
 * @Description 将映射与富化后的流量数据写入kafka
 * @author xudong
 * @Date 2018-11-01
 */


public class KafkaProducerBolt extends BaseRichBolt {
	
	//手动指定序列化id，防止后续类修改导致反序列化失败
	private static final long serialVersionUID = -2639126860311224000L;
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerBolt.class);
	
    static {
        System.out.println("--------------------MappingAndEnrichmentBolt-------------SystemMapEnrichConstants.BROKER_URL:" + SystemMapEnrichConstants.BROKER_URL);
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
            		SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }
    
    private static Properties kafkaProducerProperties = new Properties();
	private String topic;
	private String topicProperties;
	
    // private OutputCollector outputCollector;
    private static KafkaProducer<String, byte[]> producer = null;
    
    //初始化 KafkaProducer
    static {
    	try {
    		logger.info("KafkaProducerProperties init");
    		kafkaProducerProperties.put("bootstrap.servers", SystemMapEnrichConstants.BROKER_URL);
    		kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    		kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProducerProperties.put("batch.size", 1);// 16384
            kafkaProducerProperties.put("linger.ms", 1);
            kafkaProducerProperties.put("buffer.memory", 33554432);
            kafkaProducerProperties.put("acks", "0");
            kafkaProducerProperties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy、lz4。
            kafkaProducerProperties.put("topic.properties.fetch.enable", "true");
            
            //kerberos 授权 & 认证
            if(SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
                kafkaProducerProperties.put("security.protocol", "SASL_PLAINTEXT");
                kafkaProducerProperties.put("sasl.kerberos.service.name", "kafka");
            }
            
            producer = new KafkaProducer<String, byte[]>(kafkaProducerProperties);
            
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

	public KafkaProducerBolt(String topicOutput) {
		
		topic = topicOutput;
		topicProperties = producer.getTopicProperties(topic);
		System.out.println("--------------------------topic: " + topic + "topicProperties: " + topicProperties);
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		
		long begin = System.currentTimeMillis();
		Map<String, Object> syslogMap = (Map<String, Object>) input.getValue(0);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        
		long avroCompressBegin = System.currentTimeMillis();
		
		if(syslogMap != null && syslogMap.size() > 0) { 
            System.out.println("--------------------[" + topic + "] KafkaProcuderBolt syslogMap: "
                    + JsonUtils.mapToJson(syslogMap));
            
    		Schema toppicSchema = new Schema.Parser().parse(topicProperties);
    		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(toppicSchema);
    		
    		GenericRecord record = new GenericData.Record(toppicSchema);
    		
    		try {
    			for(Map.Entry<String, Object> entry : syslogMap.entrySet()) {
    				record.put(entry.getKey(), entry.getValue());
    			}
    			writer.write(record, encoder);
    	        encoder.flush();
    	        out.flush();
    	        
                long avroCompressEnd = System.currentTimeMillis();
                System.out.println("###########################KafkaProcuderBolt, avroCompress use time: "
                 + (avroCompressEnd - avroCompressBegin) + "ms, syslogMap.size():" +
                 syslogMap.size() + "&&&&& topic name: " + topic);

                long compressBegin = System.currentTimeMillis();
                byte[] sendData = out.toByteArray();
                // TODO:
                // byte[] sendData = SnappyCompress.compress(out.toByteArray());

                // System.out.println("----------------------------out.toByteArray().length:" + out.toByteArray().length);
                // byte[] sendData = SnappyCompress.compress001(out.toByteArray());
                // System.out.println("----------------------------SnappyCompress.compress001(out.toByteArray()).length:" +
                // SnappyCompress.compress001(out.toByteArray()).length);
                long compressEnd = System.currentTimeMillis();
                System.out.println("###########################KafkaProcuderBolt, compress use time: "
                + (compressEnd - compressBegin) + "ms");

                long sendBegin = System.currentTimeMillis();
                // TODO: test
                producer.send(new ProducerRecord<String, byte[]>(topic, null, sendData));
                long sendEnd = System.currentTimeMillis();
                System.out.println("###########################KafkaProcuderBolt, send use time: "
                + (sendEnd - sendBegin) + "ms");

                // producer.send(new ProducerRecord<String, byte[]>(topic, null, "sendData".getBytes()));
                // System.out.println("------------KafkaProcuderBolt---topic:" + topic + "--------sendData.length:" +
                // sendData.length);
                // ------------KafkaProcuderBolt---topic:ty_tcpflow_outputtest1--------sendData.length:1646000
                // ------------KafkaProcuderBolt---topic:ty_tcpflow_outputtest1--------sendData.length:94412

                long end = System.currentTimeMillis();
                System.out
                        .println("#################################################################################KafkaProcuderBolt, use time: "
                                + (end - begin) + "ms");
    		}catch(Exception e) {
                e.printStackTrace();
                System.out.println("--------------------[" + topic + "] syslogMap: " + JsonUtils
                        .mapToJson(syslogMap) + ",  " + e.getMessage());
    		}
		}
		
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
