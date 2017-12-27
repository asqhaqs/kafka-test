
package soc.storm.situation.test.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import soc.storm.situation.utils.JsonUtils;

/**
 * 
 * @author wangbin03
 *
 */
public class AvroSerializerTest {

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

    public static JSONArray getJsonArray002() {
        JSONArray jsonArray = new JSONArray();
        JSONObject object1 = new JSONObject();
        object1.put("name", "111cstring");
        JSONObject object1Child = new JSONObject();
        object1Child.put("notafter", "111");
        JSONArray jsonArraySub = new JSONArray();
        jsonArraySub.add(object1Child);
        object1.put("cert", jsonArraySub);

        jsonArray.add(object1);
        return jsonArray;
    }

    public static Schema getSubSchemaElementType(Schema topicSchema) {
        Schema subSchema = null;
        Schema.Field subField = topicSchema.getField("cert");
        List<Schema> schemaList = subField.schema().getTypes();
        for (Schema schema : schemaList) {
            if (schema.getType().equals(Type.ARRAY)) {
                subSchema = schema;
            }
        }
        return subSchema.getElementType();
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        String skyeyeWebFlowLogStr = "{\"server_name\":\"secclientgw.alipay.com\",\"partition_time\":\"2017-12-25 14\",\"user_name\":\"/C=CN/L=Shanghai/O=Alipay.com Co.,Ltd/OU=Operations Departments/CN=*.alipay.com\",\"geo_sip\":{},\"session_id\":\"87204bcacabf404046c46819902d487b36f73b88ad24f565a17f22f7890dc628\",\"dip\":\"110.76.30.69\",\"cert\":[{\"public_key\":\"3082010a02820101009b2d8ee14dffd1c089edff8f142e36f6e8320e78b6b56424603f9c119209d14e1f4f892daf24504e05cc0499f0d988a3a9a62408f75bf1266a1dc856fbf0092cc465e3f26a94ac31faea67f5e4fce9d7040ee01d143138fe12ae189cc9759b84d916d6a27213e0e7cc2a3f7879808cb59f90653041005bf6041a41cc2f363255c4019f6eb36c552ca78c7a94bd504bb8478dff43e84e8b8cec538d50fda05fc9a3e2aef194e40866039a63f6a0ff632751d8e50604009345e8a05442d3824a5c279fc6f3debb64d8234136e7a472084b231f8360b36276a24f6be6bbb9cb15819a19dd4eeea9808ce60f425bdf529abd3a1684e700ec27c140cdb303120ab3d50203010001\",\"notbefore\":\"UTC 2017-11-17 00:00:00\",\"notafter\":\"UTC 2018-08-01 23:59:59\",\"issuer_name\":\"/C=US/O=Symantec Corporation/OU=Symantec Trust Network/CN=Symantec Class 3 Secure Server CA - G4\"},{\"public_key\":\"3082010a0282010100b2d805ca1c742db5175639c54a520996e84bd80cf1689f9a422862c3a530537e5511825b037a0d2fe17904c9b496771981019459f9bcf77a9927822db783dd5a277fb2037a9c5325e9481f464fc89d29f8be7956f6f7fdd93a68da8b4b82334112c3c83cccd6967a84211a22040327178b1c6861930f0e5180331db4b5ceeb7ed062aceeb37b0174ef6935ebcad53da9ee9798ca8daa440e25994a1596a4ce6d02541f2a6a26e2063a6348acb44cd1759350ff132fd6dae1c618f59fc9255df3003ade264db42909cd0f3d236f164a8116fbf28310c3b8d6d855323df1bd0fbd8c52954a16977a522163752f16f9c466bef5b509d8ff2700cd447c6f4b3fb0f70203010001\",\"notbefore\":\"UTC 2013-10-31 00:00:00\",\"notafter\":\"UTC 2023-10-30 23:59:59\",\"issuer_name\":\"/C=US/O=VeriSign, Inc./OU=VeriSign Trust Network/OU=(c) 2006 VeriSign, Inc. - For authorized use only/CN=VeriSign Class 3 Public Primary Certification Authority - G5\"},{\"public_key\":\"3082010a0282010100af240808297a359e600caae74b3b4edc7cbc3c451cbb2be0fe2902f95708a364851527f5f1adc831895d22e82aaaa642b38ff8b955b7b1b74bb3fe8f7e0757ecef43db66621561cf600da4d8def8e0c362083d5413eb49ca59548526e52b8f1b9febf5a191c23349d843636a524bd28fe870514dd189697bc770f6b3dc1274db7b5d4b56d396bf1577a1b0f4a225f2af1c926718e5f40604ef90b9e400e4dd3ab519ff02baf43ceee08beb378becf4d7acf2f6f03dafdd759133191d1c40cb7424192193d914feac2a52c78fd50449e48d6347883c6983cbfe47bd2b7e4fc595ae0e9dd4d143c06773e314087ee53f9f73b8330acf5d3f3487968aee53e825150203010001\",\"notbefore\":\"UTC 2006-11-08 00:00:00\",\"notafter\":\"UTC 2021-11-07 23:59:59\",\"issuer_name\":\"/C=US/O=VeriSign, Inc./OU=Class 3 Public Primary Certification Authority\"}],\"serial_num\":\"214585853\",\"version\":\"TLS 1.2\",\"dport\":443,\"found_time\":1514183555094,\"geo_dip\":{\"subdivision\":\"Zhejiang Sheng\",\"city_name\":\"Hangzhou\",\"timezone\":\"Asia/Shanghai\",\"latitude\":\"30.2936\",\"country_code2\":\"CN\",\"country_name\":\"China\",\"continent_code\":\"AS\",\"longitude\":\"120.1614\"},\"sip\":\"10.74.66.173\",\"sport\":62652,\"access_time\":\"2017-12-18 11:05:22.488\"}";
        Map<String, Object> skyeyeWebFlowLogMap = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);

        // String topicProperties =
        // "{\"type\":\"record\",\"name\":\"user\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"cert\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"skyeye_ssl_cert\",\"fields\":[{\"name\":\"notafter\",\"type\":[\"string\",\"null\"]}]}}}]}";

        // String topicProperties =
        // "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"user\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

        String topicProperties =
                "{\"namespace\":\"skyeye_ssl\",\"type\":\"record\",\"name\":\"skyeye_ssl\",\"fields\":[{\"type\":[\"long\",\"null\"],\"name\":\"found_time\"},{\"type\":[\"string\",\"null\"],\"name\":\"sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"server_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"access_time\"},{\"type\":[{\"items\":{\"fields\":[{\"type\":[\"string\",\"null\"],\"name\":\"issuer_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"notbefore\"},{\"type\":[\"string\",\"null\"],\"name\":\"notafter\"},{\"type\":[\"string\",\"null\"],\"name\":\"public_key\"}],\"type\":\"record\",\"name\":\"element\"},\"type\":\"array\"},\"null\"],\"name\":\"cert\"},{\"type\":[\"string\",\"null\"],\"name\":\"session_id\"},{\"type\":[\"string\",\"null\"],\"name\":\"serial_num\"},{\"type\":[\"string\",\"null\"],\"name\":\"sipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"dipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"version\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"user_name\"},{\"type\":[\"int\",\"null\"],\"name\":\"dport\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_dip\"},{\"type\":[\"int\",\"null\"],\"name\":\"sport\"},{\"type\":[\"string\",\"null\"],\"name\":\"dip\"},{\"type\":[\"string\"],\"name\":\"partition_time\"}]}";
        // String topicProperties =
        // "{\"namespace\":\"skyeye_ssl\",\"type\":\"record\",\"name\":\"skyeye_ssl\",\"fields\":[{\"type\":[\"long\",\"null\"],\"name\":\"found_time\"},{\"type\":[\"string\",\"null\"],\"name\":\"sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"server_name\"},{\"type\":[\"string\",\"null\"],\"name\":\"access_time\"},{\"type\":[{\"items\":{\"values\":\"string\",\"type\":\"map\"},\"type\":\"array\"},\"null\"],\"name\":\"cert\"},{\"type\":[\"string\",\"null\"],\"name\":\"session_id\"},{\"type\":[\"string\",\"null\"],\"name\":\"serial_num\"},{\"type\":[\"string\",\"null\"],\"name\":\"sipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"dipv6\"},{\"type\":[\"string\",\"null\"],\"name\":\"version\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_sip\"},{\"type\":[\"string\",\"null\"],\"name\":\"user_name\"},{\"type\":[\"int\",\"null\"],\"name\":\"dport\"},{\"type\":[{\"values\":\"string\",\"type\":\"map\"},\"null\"],\"name\":\"geo_dip\"},{\"type\":[\"int\",\"null\"],\"name\":\"sport\"},{\"type\":[\"string\",\"null\"],\"name\":\"dip\"},{\"type\":[\"string\"],\"name\":\"partition_time\"}]}";

        Schema topicSchema = new Schema.Parser().parse(topicProperties);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(topicSchema);

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            // // JSONArray jsonArray = getJsonArray();
            // JSONArray jsonArray = getJsonArray002();
            // for (int i = 0; i < jsonArray.size(); i++) {
            // GenericRecord record = new GenericData.Record(topicSchema);
            // JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            //
            // // Object cint = jsonObject.remove("cint");
            // // record.put("cint", cint);
            // // Object cstring = jsonObject.remove("cstring");
            // // record.put("cstring", cstring);
            // // Object cmap = jsonObject.remove("cmap");
            // // record.put("cmap", cmap);
            //
            // Object name = jsonObject.remove("name");
            // record.put("name", name);
            // // Object cmap = jsonObject.remove("cert");
            // // record.put("cert", cmap);
            //
            // datumWriter.write(record, encoder);
            // }

            if (null != skyeyeWebFlowLogMap && 0 != skyeyeWebFlowLogMap.size()) {
                GenericRecord record = new GenericData.Record(topicSchema);
                for (Map.Entry<String, Object> entry : skyeyeWebFlowLogMap.entrySet()) {
                    if (entry.getKey().equals("cert")) {
                        // {
                        // "public_key":
                        // "3082010a02820101009b2d8ee14dffd1c089edff8f142e36f6e8320e78b6b56424603f9c119209d14e1f4f892daf24504e05cc0499f0d988a3a9a62408f75bf1266a1dc856fbf0092cc465e3f26a94ac31faea67f5e4fce9d7040ee01d143138fe12ae189cc9759b84d916d6a27213e0e7cc2a3f7879808cb59f90653041005bf6041a41cc2f363255c4019f6eb36c552ca78c7a94bd504bb8478dff43e84e8b8cec538d50fda05fc9a3e2aef194e40866039a63f6a0ff632751d8e50604009345e8a05442d3824a5c279fc6f3debb64d8234136e7a472084b231f8360b36276a24f6be6bbb9cb15819a19dd4eeea9808ce60f425bdf529abd3a1684e700ec27c140cdb303120ab3d50203010001",
                        // "notbefore": "UTC 2017-11-17 00:00:00",
                        // "notafter": "UTC 2018-08-01 23:59:59",
                        // "issuer_name":
                        // "/C=US/O=Symantec Corporation/OU=Symantec Trust Network/CN=Symantec Class 3 Secure Server CA - G4"
                        // },

                        Schema subSchemaElementType = null;
                        List<GenericRecord> subRecordArray = new ArrayList<GenericRecord>();

                        List<Map<String, Object>> subRecordMapList = (List<Map<String, Object>>) entry.getValue();
                        if (subRecordMapList != null && subRecordMapList.size() > 0) {
                            subSchemaElementType = AvroSerializerTest.getSubSchemaElementType(topicSchema);
                        }
                        for (Map<String, Object> subRecordMap : subRecordMapList) {
                            GenericRecord subRecord = new GenericData.Record(subSchemaElementType);
                            for (Map.Entry<String, Object> subRecordEntry : subRecordMap.entrySet()) {
                                subRecord.put(subRecordEntry.getKey(), subRecordEntry.getValue());
                            }

                            subRecordArray.add(subRecord);
                        }

                        record.put("cert", subRecordArray);
                    } else {
                        record.put(entry.getKey(), entry.getValue());
                    }
                }
                datumWriter.write(record, encoder);
            }

            encoder.flush();
            byte[] sendData = out.toByteArray();
            System.out.println(sendData.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
