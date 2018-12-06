package cn.situation.util;

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
        String str = "aa|bb|dd|1233|ss%%%aa|333|111====";
        System.out.println(str.replaceAll("%%%", "|"));
    }
}