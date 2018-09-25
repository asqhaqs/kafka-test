
package soc.storm.situation.test.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;

public class KafkaAvroTest {

    public static final String WEBATTACK_SCHEMA_STRING = "{\"namespace\":\"skyeye_webattack_dolog\",\"type\":\"record\",\"name\":\"skyeye_webattack_dolog\",\"fields\":[{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_victim\"},{\"type\":[\"int\",\"null\"],\"name\":\"write_date\"},{\"type\":[\"string\",\"null\"],\"name\":\"file_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"req_header\"},{\"type\":[\"string\",\"null\"],\"name\":\"es_version\"},{\"type\":[\"string\",\"null\"],\"name\":\"agent\"},{\"type\":[\"string\",\"null\"],\"name\":\"rule_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"host\"},{\"type\":[\"int\",\"null\"],\"name\":\"dolog_count\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"victim\"},{\"type\":[\"long\",\"null\"],\"name\":\"es_timestamp\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_dip\"},{\"type\":[\"int\",\"null\"],\"name\":\"sport\"},{\"type\":[\"string\",\"null\"],\"name\":\"victim_type\"},{\"type\":[\"string\",\"null\"],\"name\":\"attack_type\"},{\"type\":[\"int\",\"null\"],\"name\":\"rsp_status\"},{\"type\":[\"string\",\"null\"],\"name\":\"sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"severity\"},{\"type\":[\"int\",\"null\"],\"name\":\"rsp_body_len\"},{\"type\":[\"string\",\"null\"],\"name\":\"attacker\"},{\"type\":[\"string\",\"null\"],\"name\":\"event_id\"},{\"type\":[\"string\",\"null\"],\"name\":\"parameter\"},{\"type\":[\"string\",\"null\"],\"name\":\"attack_flag\"},{\"type\":[\"string\",\"null\"],\"name\":\"uri\"},{\"type\":[\"int\",\"null\"],\"name\":\"rsp_content_length\"},{\"type\":[\"string\",\"null\"],\"name\":\"serial_num\"},{\"type\":[\"string\",\"null\"],\"name\":\"rsp_content_type\"},{\"type\":[\"string\",\"null\"],\"name\":\"method\"},{\"type\":[\"int\",\"null\"],\"name\":\"rule_version\"},{\"type\":[\"string\",\"null\"],\"name\":\"cookie\"},{\"type\":[\"string\",\"null\"],\"name\":\"rsp_body\"},{\"type\":[\"string\",\"null\"],\"name\":\"rsp_header\"},{\"type\":[\"int\",\"null\"],\"name\":\"dport\"},{\"type\":[\"string\",\"null\"],\"name\":\"referer\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_attacker\"},{\"type\":[\"string\",\"null\"],\"name\":\"dip\"},{\"type\":[\"int\",\"null\"],\"name\":\"rule_id\"},{\"type\":[\"string\",\"null\"],\"name\":\"req_body\"},{\"type\":[\"string\"],\"name\":\"hive_partition_time\"}]}";

    private static final Schema WEBATTACK_SCHEMA = new Schema.Parser().parse(WEBATTACK_SCHEMA_STRING);

    public static final GenericDatumWriter<GenericRecord> WRITER = new GenericDatumWriter<GenericRecord>(WEBATTACK_SCHEMA);

    public static final GenericDatumReader<GenericRecord> READER = new GenericDatumReader<GenericRecord>(WEBATTACK_SCHEMA);

    /**
     * 
     * @param content
     * @return
     * @throws Exception
     */
    public byte[] serialize(GenericRecord content) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        WRITER.write(content, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * 
     * @param array
     * @return
     * @throws Exception
     */
    public GenericRecord deserialize(byte[] array) throws Exception {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(array, null);
        return READER.read(null, decoder);
    }

    private static byte[] fromJsonToAvro(String json, Schema schema) throws Exception {
        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);

            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, din);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
            Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
            Object datum = null;
            while (true) {
                try {
                    datum = reader.read(datum, jsonDecoder);
                } catch (EOFException eofException) {
                    break;
                }
                writer.write(datum, e);
                e.flush();
            }
        } finally {
            // Util.close(input);
        }

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        Object datum = reader.read(null, decoder);

        GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
        // ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }

    public static void main(String args[]) {
        AvroGenericSerializerTest avroGenericSerializer = new AvroGenericSerializerTest();
        try {
            // GenericRecord genericRecord = avroGenericSerializer.create001();
            // GenericRecord genericRecord = avroGenericSerializer.create002();
            // GenericRecord genericRecord = avroGenericSerializer.create003();
            GenericRecord genericRecord = avroGenericSerializer.create004();

            System.out.println("genericRecord:" + genericRecord);

            byte[] bytes = avroGenericSerializer.serialize(genericRecord);
            for (int i = 0; i < bytes.length; i++) {
                System.out.println(bytes[i]);
            }
            System.out.println("编码完成");

            GenericRecord grback = avroGenericSerializer.deserialize(bytes);
            System.out.println(grback);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
