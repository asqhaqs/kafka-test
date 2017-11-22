
package soc.storm.situation.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Created by yuhaiyang on 2017/9/26.
 */
public class KafkaProducerTest {

    private static Properties kafkaProducerProperties = new Properties();
    // private static String json =
    // "[{\"cint\":1, \"cstring\":\"111strvalue\"},{\"cint\":2, \"cstring\":\"222strvalue\"}]";
    //private static String topic = "test4";
    private static String topic = "ty_dns_enrichment";

    public static JSONArray getJsonArray() {
        JSONArray jsonArray = new JSONArray();
        JSONObject object1 = new JSONObject();
        object1.put("cint", 1111);
        object1.put("cstring", "111cstring");
        JSONObject object1Child = new JSONObject();
        object1Child.put("c1", "111");
        object1Child.put("c2", "222");
        object1.put("cmap", object1Child);

        JSONObject object2 = new JSONObject();
        object2.put("cint", 2);
        object2.put("cstring", "222cstring");
        JSONObject object2Child = new JSONObject();
        object2Child.put("c1", "333");
        object2Child.put("c2", "444");
        object2.put("cmap", object2Child);

        jsonArray.add(object1);
        jsonArray.add(object2);
        return jsonArray;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        //kafkaProducerProperties.put("bootstrap.servers", "tempt43.ops.lycc.qihoo.net:9092,tempt44.ops.lycc.qihoo.net:9092");
        kafkaProducerProperties.put("bootstrap.servers", "bjwa1.tianyan.bjyt.qihoo.net:6667");
        kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put("batch.size", 1);
        kafkaProducerProperties.put("linger.ms", 1);
        kafkaProducerProperties.put("buffer.memory", 33554432);// 32M
        kafkaProducerProperties.put("acks", "0");
        kafkaProducerProperties.put("compression.type", "snappy");// #消息压缩模式，默认是none，可选gzip、snappy。
        //kafkaProducerProperties.put("topic.properties.fetch.enable", "true");

        // org.apache.kafka.common.serialization.StringSerializer

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(kafkaProducerProperties);
        // String topicProperties = producer.getTopicProperties(topic);
//        String topicProperties = null;// producer.getTopicProperties(topic);
//
//        Schema topicSchema = new Schema.Parser().parse(topicProperties);
//        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(topicSchema);

        // try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        // BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        // JSONArray jsonArray = getJsonArray();
        // for (int i = 0; i < jsonArray.size(); i++) {
        // GenericRecord record = new GenericData.Record(topicSchema);
        // JSONObject jsonObject = (JSONObject) jsonArray.get(i);
        //
        // Object cint = jsonObject.remove("cint");
        // record.put("cint", cint);
        // Object cstring = jsonObject.remove("cstring");
        // record.put("cstring", cstring);
        // Object cmap = jsonObject.remove("cmap");
        // record.put("cmap", cmap);
        //
        // datumWriter.write(record, encoder);
        // }
        // encoder.flush();
        // byte[] sendData = out.toByteArray();
        // Future<RecordMetadata> future = producer.send(new ProducerRecord<String, byte[]>(topic, null, sendData));
        // try {
        // future.get(4, TimeUnit.SECONDS);
        // } catch (TimeoutException e) {
        // e.printStackTrace();
        // }
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        //ByteArrayOutputStream out = null;
        try {
//            out = new ByteArrayOutputStream();
//            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//            JSONArray jsonArray = getJsonArray();
//            for (int i = 0; i < jsonArray.size(); i++) {
//                GenericRecord record = new GenericData.Record(topicSchema);
//                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
//
//                Object cint = jsonObject.remove("cint");
//                record.put("cint", cint);
//                Object cstring = jsonObject.remove("cstring");
//                record.put("cstring", cstring);
//                Object cmap = jsonObject.remove("cmap");
//                record.put("cmap", cmap);
//
//                datumWriter.write(record, encoder);
//            }
//            encoder.flush();
            //byte[] sendData = out.toByteArray();
            String sendData= "{\"serial_num\":\"000005\", \"access_time\":\"2017-11-21 14:08:00\", \"sip\":\"192.168.1.1\", \"sport\":\"5502\", \"dip\":\"192.168.10.1\", \"dport\":\"\", \"dns_type\": \"type1\", \"host\": \"www.lenzhao.com\", \"host_md5\": \"fdsfrvgffffddd\", \"addr\": \"beijing\", \"mx\": \"100\", \"cname\": \"lenzhao\", \"reply_code\": \"400\", \"count\": \"10\", \"geo_sip\": {\"gip\": \"192.168.8.8\"}, \"geo_dip\": {\"gip\": \"192.168.8.8\"}}";
            Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, null, sendData));
            try {
                future.get(4, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        } /*catch (IOException e) {
            e.printStackTrace();
        } */finally {
            /*if (null != out) {
                out.close();
            }*/
        }

    }

}
