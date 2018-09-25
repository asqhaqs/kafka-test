
package soc.storm.situation.test;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;

import soc.storm.situation.utils.DateTimeUtils;
import soc.storm.situation.utils.JsonUtils;

// import org.apache.commons.codec.digest.DigestUtils;

public class CommonTest {

    public static void main(String[] args) {
        // md5Test();
        dateTimeFormat();
        // strToByteArray();
        // testJsonToMap();
    }

    public static void changeCharset() {
        try {
            String str = "中国菜刀变形";
            byte[] bs = str.getBytes("UTF-8");
            String s = new String(bs, "UTF-8");
            System.out.println(s);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void md5Test() {
        /**
         * 刘创 201801231410
         * "host": "client.api.ttpod.com",
         * "host_md5": "74175e132f3db71c4c166a835063ce5b",
         * https://md5jiami.51240.com/
         * 32位小写
         **/

        System.out.println(DigestUtils.md5Hex("client.api.ttpod.com").toLowerCase());
        // 74175e132f3db71c4c166a835063ce5b
    }

    public static void dateTimeFormat() {
        System.out.println("DateTimeUtils.formatNowTime():" + DateTimeUtils.formatNowTime());
        System.out.println("DateTimeUtils.formatNowMonthTime():" + DateTimeUtils.formatNowMonthTime());
        System.out.println("DateTimeUtils.formatNowDayTime():" + DateTimeUtils.formatNowDayTime());
        System.out.println("DateTimeUtils.formatNowHourTime():" + DateTimeUtils.formatNowHourTime());
    }

    public static void strToByteArray() {
        String dataSourceStr = "aabbcc";
        byte[] dataSourceStrByteArray = dataSourceStr.getBytes();
        for (byte b : dataSourceStrByteArray) {
            System.out.print((char) b);
        }
        System.out.println();
    }

    public static void testJsonToMap() {
        // String skyeyeWebFlowLogStr = "{\"name\": \"c\\ad\"}";
        // String skyeyeWebFlowLogStr = "{\"name\": \"大c\\ad\"}";
        String skyeyeWebFlowLogStr1 = "{\"name\": \"大大大大大\"}";
        String skyeyeWebFlowLogStr2 = "{\"name\": \"\u5927\u5927\u5927\u5927\u5927\"}";
        System.out.println("------------skyeyeWebFlowLogStr1.length():" + skyeyeWebFlowLogStr1.length()
                + "------------skyeyeWebFlowLogStr2.length():" + skyeyeWebFlowLogStr2.length());
        // String skyeyeWebFlowLogStr = "{\"name\": \"\\u5927\\u5927\\u5927\\u5927\\u5927\"}";
        String skyeyeWebFlowLogStr = "{\"name\": \"\u5927\u5927\u5927\u5927\u5927\"}";
        Map<String, Object> skyeyeWebFlowLog = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);
        System.out.println("------------skyeyeWebFlowLog:" + skyeyeWebFlowLog);
    }
}
