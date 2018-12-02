package cn.situation.util;

import com.alibaba.fastjson.JSON;

import java.util.*;

public class JsonUtil {

    /**
     * json字符串转换为object
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T jsonToObject(String json, Class<T> clazz) {
        T t = null;
        try {
            t = JSON.parseObject(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }

    /**
     * object转换为json字符串
     * @param object
     * @return
     */
    public static String objectToJson(Object object) {
        return JSON.toJSONString(object);
    }

    /**
     * json to map
     * @param json
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> jsonToMap(String json) {
        try {
            return JSON.parseObject(json, Map.class);
        } catch (Exception e) {
            // System.out.println("------------------------skyeyeWebFlowLogStr:" + json);
            e.printStackTrace();
            // //
            // // Gson gson = new Gson();
            // JsonParser parser = new JsonParser();
            // JsonElement jsonElement = parser.parse(json);
            // // System.out.println("------------------------skyeyeWebFlowLogStr:" + json + ", jsonElement:" +
            // // jsonElement);
        }
        return null;
    }

    /**
     * map To Json
     * @param map
     * @return
     */
    public static String mapToJson(Map<String, Object> map) {
        return JSON.toJSONString(map);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("english", "aabbcc");
        dataMap.put("chinese", "天眼");
        dataMap.put("boolean", true);
        dataMap.put("Integer", 28);
        dataMap.put("Double", 28.55);
        dataMap.put("Date", new Date());

        List<String> listSrc = new ArrayList<String>();
        listSrc.add("1");
        listSrc.add("2");
        listSrc.add("3");
        // dataMap.put("list", listSrc);
        // System.out.println(JSON.toJSONString(listSrc).replace("\\", ""));
        // dataMap.put("list", JSON.toJSONString(listSrc).replace("\\", ""));
        dataMap.put("list", JSON.toJSONString(listSrc));
        String jsonString = JSON.toJSONString(dataMap).replace("\\", "");
        System.out.println(jsonString);

        Map<?, ?> object = jsonToMap(jsonString);
        System.out.println(object.get("english"));
        System.out.println(object.get("chinese"));
        System.out.println(object.get("boolean"));
        System.out.println(object.get("Integer"));
        System.out.println(object.get("Double"));
        System.out.println(object.get("Date"));
        List<String> listDes = (List<String>) object.get("list");
        System.out.println(listDes);
    }
}
