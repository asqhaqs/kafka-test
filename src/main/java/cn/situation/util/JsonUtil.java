package cn.situation.util;

import com.alibaba.fastjson.JSON;
import net.sf.json.JSONObject;
import org.slf4j.Logger;

import java.util.*;

public class JsonUtil {

    private static final Logger LOG = LogUtil.getInstance(JsonUtil.class);

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
            LOG.error(e.getMessage(), e);
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
            LOG.error(e.getMessage(), e);
        }
        return null;
    }

    public static String pack2Json(String filePath, String fileName, String kind, String type) {
        JSONObject json = new JSONObject();
        json.put("filePath", filePath);
        json.put("fileName", fileName);
        json.put("kind", kind);
        json.put("type", type);
        return json.toString();
    }

    /**
     * map To Json
     * @param map
     * @return
     */
    public static String mapToJson(Map<String, Object> map) {
        return JSON.toJSONString(map);
    }

    public static void main(String[] args) {
    }
}
