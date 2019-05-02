package cn.situation.util;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lenzhao
 * @date 2018/11/14 17:18
 * @description TODO
 */
public class LogUtil {

    public static Logger getInstance(String name) {
        return LoggerFactory.getLogger(name);
    }

    public static Logger getInstance(Class clazz) {
        return LoggerFactory.getLogger(clazz);
    }

    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        json.put("source", "{\"sess_id\":\"111111111111112222222222222222\"}");
        json.put("type", "skyeye-weblog");
        json.put("index", "skyeye-weblog-2019.05.01");
        JSONArray array = new JSONArray();
        array.add(json);
        System.out.println(array.toString());
    }
}