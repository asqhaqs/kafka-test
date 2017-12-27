
package soc.storm.situation.test.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;

public class AvroGenericSerializerTest {

    public static final String example001 = "{\"type\": \"record\", \"name\": \"Media\", \"fields\": [{\"name\": " +
            "\"uri\", \"type\": \"string\"}, {\"name\": \"title\", \"type\": " +
            "\"string\"}, {\"name\": \"width\", \"type\": \"int\"}, {\"name\": " +
            "\"height\", \"type\": \"int\"}, {\"name\": \"format\", \"type\": " +
            "\"string\"}, {\"name\": \"duration\", \"type\": \"long\"}, " +
            "{\"name\": \"size\", \"type\": \"long\"}, {\"name\": \"bitrate\", " +
            "\"type\": \"int\"}, {\"name\": \"player\", " +
            "\"type\": \"int\"}, {\"name\": \"copyright\", \"type\": \"string\"}]}";

    public static final String example002 =
            "{\"type\":\"record\",\"name\":\"skyeye_ssl\",\"namespace\":\"skyeye_ssl\",\"fields\":[{\"name\":\"found_time\",\"type\":[\"long\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"cert\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"namespace\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"issuer_name\",\"type\":\"string\"},{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]}]}}}]}";

    public static final String example003 =
            "{\"type\":\"record\",\"name\":\"skyeye_ssl\",\"namespace\":\"skyeye_ssl\",\"fields\":[{\"name\":\"found_time\",\"type\":[\"long\",\"null\"]},{\"name\":\"cert\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"namespace\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"issuer_name\",\"type\":\"string\"},{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]}]}}}]}";

    public static final String example002SubSSL = "{\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"namespace\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"issuer_name\",\"type\":\"string\"},{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]}]}";

    public static final String example002SubString = "{\"type\":\"record\",\"name\":\"skyeye_ssl\",\"namespace\":\"skyeye_ssl\",\"fields\":[{\"name\":\"found_time\",\"type\":[\"long\",\"null\"]},{\"name\":\"sip\",\"type\":[\"string\",\"null\"]},{\"name\":\"cert\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";

    private static String topicProperties017 = "[{\"namespace\":\"skyeye_ssl_cert\",\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"issuer_name\",\"type\":\"string\"},{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]},{\"name\":\"notbefore\",\"type\":[\"string\",\"null\"]},{\"name\":\"public_key\",\"type\":[\"string\",\"null\"]}]},{\"namespace\":\"skyeye_ssl\",\"type\":\"record\",\"name\":\"skyeye_ssl\",\"fields\":[{\"type\":[\"long\",\"null\"],\"name\":\"found_time\"},{\"type\":[\"string\",\"null\"],\"name\":\"sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"server_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"access_time\"},{\"type\":{\"type\":\"array\",\"items\":\"skyeye_ssl_cert.skyeye_ssl_cert\"},\"name\":\"cert\"},{\"type\":[\"string\",\"null\"],\"name\":\"session_id\"},{\"type\":[\"string\",\"null\"],\"name\":\"serial_num\"},{\"type\":[\"string\",\"null\"],\"name\":\"sipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"dipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"version\"},{\"name\":\"geo_sip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"type\":[\"string\",\"null\"],\"name\":\"user_name\"},{\"type\":[\"int\",\"null\"],\"name\":\"dport\"},{\"name\":\"geo_dip\",\"type\":[{\"type\":\"map\",\"values\":\"string\"},\"null\"]},{\"type\":[\"int\",\"null\"],\"name\":\"sport\"},{\"type\":[\"string\",\"null\"],\"name\":\"dip\"},{\"type\":[\"string\"],\"name\":\"partition_time\"}]}]";

    private static final Schema MEDIA_SCHEMA = new Schema.Parser().parse(example002SubString);

    private static final Schema MEDIA_SCHEMA_SUB = new Schema.Parser().parse(example002SubSSL);

    private static String userSchema = "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"user\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
    private static String userSchemaArray = "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"user\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]},{\"name\":\"cert\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"namespace\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"issuer_name\",\"type\":\"string\"},{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]}]}}}]}";
    private static final Schema User_Schema = new Schema.Parser().parse(userSchemaArray);

    public static final GenericDatumWriter<GenericRecord> WRITER = new GenericDatumWriter<GenericRecord>(MEDIA_SCHEMA);

    public static final GenericDatumReader<GenericRecord> READER = new GenericDatumReader<GenericRecord>(MEDIA_SCHEMA);

    public GenericRecord create001() throws Exception {
        GenericRecord media = new GenericData.Record(MEDIA_SCHEMA);
        media.put("uri", new Utf8("http://javaone.com/keynote.mpg"));
        media.put("format", new Utf8("video/mpg4"));
        media.put("title", new Utf8("Javaone Keynote"));
        media.put("duration", 1234567L);
        media.put("bitrate", 0);

        media.put("player", 1);
        media.put("height", 0);
        media.put("width", 0);
        media.put("size", 123L);
        media.put("copyright", new Utf8());

        return media;
    }

    public GenericRecord create002() throws Exception {
        GenericRecord media = new GenericData.Record(MEDIA_SCHEMA);
        media.put("found_time", 123L);
        // media.put("found_time", new Utf8("123L"));
        media.put("sip", new Utf8("192.168.1.1"));

        List<String> stringArray = new ArrayList<String>();

        // List<GenericRecord> stringArray = new ArrayList<GenericRecord>();
        // // GenericRecord subMedia = new GenericData.Record(MEDIA_SCHEMA_SUB);
        // GenericRecord subMedia = new GenericData.Record(MEDIA_SCHEMA_SUB);
        // subMedia.put("issuer_name", new Utf8("192.168.1.1"));
        // subMedia.put("notafter", new Utf8("192.168.1.1"));
        // stringArray.add(subMedia);

        media.put("cert", stringArray);

        return media;
    }

    public GenericRecord create003() throws Exception {
        System.out.println("------------cert, type: " + MEDIA_SCHEMA.getType()
                + ", name: " + MEDIA_SCHEMA.getName());
        Schema.Field subField = MEDIA_SCHEMA.getField("cert");
        System.out.println("------------cert, type: " + subField.schema().getType()
                + ", name: " + subField.schema().getName()
                + ", elementType: " + subField.schema().getElementType());

        //
        GenericRecord media = new GenericData.Record(MEDIA_SCHEMA);
        media.put("found_time", 123L);
        media.put("sip", new Utf8("192.168.1.1"));

        //
        List<GenericRecord> stringArray = new ArrayList<GenericRecord>();
        // GenericRecord subMedia = new GenericData.Record(MEDIA_SCHEMA_SUB);
        GenericRecord subMedia = new GenericData.Record(subField.schema().getElementType());
        subMedia.put("issuer_name", new Utf8("192.168.1.1"));
        subMedia.put("notafter", new Utf8("192.168.1.1"));
        stringArray.add(subMedia);

        media.put("cert", stringArray);

        return media;
    }

    // {\"name\": \"Alyssa\", \"favorite_number\": {\"int\": 7}, \"favorite_color\": null,

    public GenericRecord create004() throws Exception {
        GenericRecord media = new GenericData.Record(User_Schema);
        media.put("name", new Utf8("192.168.1.1"));
        media.put("favorite_number", 7);
        List<String> stringArray = new ArrayList<String>();
        media.put("cert", stringArray);

        return media;
    }

    public byte[] serialize(GenericRecord content) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        WRITER.write(content, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    public GenericRecord deserialize(byte[] array) throws Exception {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(array, null);
        return READER.read(null, decoder);
    }

    /**
     * https://stackoverflow.com/questions/29625959/json-to-avro-conversion-using-java
     * 
     * @param inputString
     * @param className
     * @param schema
     * @return
     */
    public <T> T jsonDecodeToAvro(String inputString, Class<T> className, Schema schema) {
        T returnObject = null;
        try {
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, inputString);
            SpecificDatumReader<T> reader = new SpecificDatumReader<T>(className);
            returnObject = reader.read(null, jsonDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return returnObject;
    }

    private static byte[] fromJsonToAvro(String json, Schema schema) throws Exception {
        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // Schema schema;
        // InputStream input = Util.fileOrStdin(inputFile, stdin);

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

        // // InputStream input = new ByteArrayInputStream(json.getBytes());
        // // DataInputStream din = new DataInputStream(input);
        //
        // Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
        //
        // DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        // Object datum = reader.read(null, decoder);
        //
        // GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
        // // ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //
        // Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        //
        // w.write(datum, e);
        // e.flush();

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

            // byte[] bytes = avroGenericSerializer.serialize(genericRecord);
            // for (int i = 0; i < bytes.length; i++) {
            // System.out.println(bytes[i]);
            // }
            // System.out.println("编码完成");
            //
            // GenericRecord grback = avroGenericSerializer.deserialize(bytes);
            // System.out.println(grback);

            //
            // GenericRecord grback002 = avroGenericSerializer.jsonDecodeToAvro(genericRecord.toString(),
            // grback.getClass(),
            // MEDIA_SCHEMA);
            // System.out.println(grback002);
            // String user = "{\"name\": \"Alyssa\", \"favorite_number\": {\"int\": 7}, \"favorite_color\": null}";
            String userArray = "{\"name\": \"Alyssa\", \"favorite_number\": {\"int\": 7}, \"favorite_color\": null, \"cert\": []}";
            byte[] bytes002 = avroGenericSerializer.fromJsonToAvro(genericRecord.toString(), User_Schema);
            for (int i = 0; i < bytes002.length; i++) {
                System.out.println(bytes002[i]);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
