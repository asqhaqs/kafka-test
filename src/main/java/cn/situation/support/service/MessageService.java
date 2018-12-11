package cn.situation.support.service;

import cn.situation.cons.SystemConstant;
import cn.situation.util.*;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lenzhao
 * @date 2018/12/6 12:34
 * @description TODO
 */
public class MessageService {

    private static final Logger LOG = LogUtil.getInstance(MessageService.class);

    private static Map<String, String> messageHeadFieldMap = new HashMap<>();
    private static List<String> messageHeadFieldList = new ArrayList<>();
    private static int messageHeadFieldSize;
    private static Map<String, String> messageTypeMap = new HashMap<>();
    private static Map<String, Map<String, String>> metadataFieldMap = SystemConstant.getMetadataFieldMap();
    private static Map<String, Map<String, String>> metadataMappedFieldMap = SystemConstant.getMetadataMappedFieldMap();
    private static Map<String, Map<String, String>> metadataMappedTypeMap = SystemConstant.getMetadataMappedTypeMap();
    private static Map<String, Map<String, String>> metadataUnMappedFieldMap = SystemConstant.getMetadataUnMappedFieldMap();
    private static Map<String, String> metadataRedisKeyMap = new HashMap<>();
    private static RedisCache<String, String> metadataRedisCache = RedisUtil.getRedisCache(SystemConstant.METADATA_REDIS_CACHE);

    static {
        String[] headFields = SystemConstant.MESSAGE_HEAD_FIELD.split(",");
        String[] metadataRedisKeys = SystemConstant.METADATA_REDIS_KEY.split(",");
        for (String headField : headFields) {
            if (!StringUtil.isBlank(headField)) {
                String[] fieldType = headField.split(":");
                messageHeadFieldList.add(fieldType[0]);
                messageHeadFieldMap.put(fieldType[0], fieldType[1]);
            }
        }
        String[] messageTypes = SystemConstant.MESSAGE_TYPE.split(",");
        for (String messageType : messageTypes) {
            if (!StringUtil.isBlank(messageType)) {
                String[] mts = messageType.split(":");
                messageTypeMap.put(mts[0], mts[1]);
            }
        }
        messageHeadFieldSize = messageHeadFieldList.size();
        for (String redisKey : metadataRedisKeys) {
            if (!StringUtil.isBlank(redisKey)) {
                metadataRedisKeyMap.put(redisKey.split(":")[0], redisKey.split(":")[1]);
            }
        }
    }

    /**
     * 解析流量元数据
     * @param lines
     */
    public static void parseMetadata(List<String> lines, String fileName) {
        List<String> dataList = new ArrayList<>();
        String metadataType = null;
        for (String line : lines) {
            try {
                String[] values = line.split("\\|", -1);
                if (values.length <= messageHeadFieldSize) {
                    LOG.error(String.format("[%s]: line<%s>, size<%s>, headSize<%s>, fileName<%s>, message<%s>", "parseMetadata",
                            line, values.length, messageHeadFieldSize, fileName, "消息总长度应大于消息头长度."));
                    return;
                }
                String msgType = values[2];
                metadataType = messageTypeMap.get(msgType);
                if ("1".equals(SystemConstant.MONITOR_STATISTIC_ENABLED) && !StringUtil.isBlank(metadataType)) {
                    SystemConstant.MONITOR_STATISTIC.put(metadataType, (SystemConstant.MONITOR_STATISTIC.get(metadataType)+1));
                }
                if ("tcp".equals(metadataType)) {
                    // TCP/UDP特殊处理
                    metadataType = fileName.substring(0 ,3).toLowerCase();
                }
                LOG.debug(String.format("[%s]: line<%s>, msgType<%s>, metadataType<%s>, size<%s>, fileName<%s>", "parseMetadata",
                        line, msgType, metadataType, values.length, fileName));
                Map<String, String> unMappedMap = metadataUnMappedFieldMap.get(metadataType);
                Map<String, String> mappedMap = metadataMappedFieldMap.get(metadataType);
                Map<String, String> mappedTypeMap = metadataMappedTypeMap.get(metadataType);
                Map<String, String> messageFieldMap = metadataFieldMap.get(metadataType);
                List<String> messageFieldList = new ArrayList<>();
                if (null != messageFieldMap && !messageFieldMap.isEmpty()) {
                    for (Map.Entry<String, String> en : messageFieldMap.entrySet()) {
                        messageFieldList.add(en.getKey());
                    }
                }
                Map<String, String> fieldMap = new HashMap<>();
                Map<String, String> typeMap = new HashMap<>();
                Map<String, Object> map = new HashMap<>();
                if (values.length != (messageHeadFieldSize + messageFieldList.size())) {
                    LOG.error(String.format("[%s]: line<%s>, metadataType<%s>, size<%s>, msgSize<%s>, fileName<%s>, message<%s>",
                            "parseMetadata", line, metadataType, values.length,
                            (messageHeadFieldSize + messageFieldList.size()), fileName, "消息总长度与定义长度不一致."));
                    return;
                }
                for (int i = 0; i < messageHeadFieldSize; i++) {
                    String fieldName = messageHeadFieldList.get(i);
                    typeMap.put(fieldName, messageHeadFieldMap.get(fieldName));
                    fieldMap.put(fieldName, values[i]);
                }
                for (int j = 0; j < values.length - messageHeadFieldSize; j++) {
                    String fieldName = messageFieldList.get(j);
                    typeMap.put(fieldName, messageFieldMap.get(fieldName));
                    fieldMap.put(fieldName, values[messageHeadFieldSize + j]);
                }
                LOG.debug(String.format("[%s]: oriFieldMap<%s>, metadataType<%s>, size<%s>, fileName<%s>", "parseMetadata",
                        fieldMap, metadataType, fieldMap.size(), fileName));
                if (null != mappedMap && !mappedMap.isEmpty()) {
                    for (Map.Entry<String, String> en : mappedMap.entrySet()) {
                        String k1 = en.getKey();
                        String k2 = en.getValue();
                        String value = fieldMap.get(k1);
                        fieldMap.remove(k1);
                        fieldMap.put(k2, value);
                        typeMap.remove(k1);
                        typeMap.put(k2, mappedTypeMap.get(k2));
                    }
                }
                if (!fieldMap.isEmpty()) {
                    for (Map.Entry<String, String> en : fieldMap.entrySet()) {
                        map.put(en.getKey(), typpeConvert(en.getValue(), typeMap.get(en.getKey())));
                    }
                }
                addUnMappedField(unMappedMap, map);
                // GEO 富化
                // GeoUtil.enrichmentIp(map);
                if (!map.isEmpty()) {
                    if ("http".equals(metadataType)) {
                        Object data = map.getOrDefault("data", "");
                        Object resBody = map.getOrDefault("res_body", "");
                        String dataStr = (String) data;
                        if (!StringUtil.isBlank(dataStr)) {
                            map.put("data", Base64.decodeBase64(dataStr.getBytes()));
                        }
                        String resBodyStr = (String) resBody;
                        if (!StringUtil.isBlank(resBodyStr)) {
                            map.put("res_body", Base64.decodeBase64(resBodyStr.getBytes()));
                        }
                    }
                    if ("dns".equals(metadataType) || "http".equals(metadataType)) {
                        if (map.containsKey("host") && null != map.get("host") && !"".equals(map.get("host"))) {
                            map.put("host_md5", MD5Util.encodeToStr(map.get("host").toString()));
                        }
                    }
                    dataList.add(JsonUtil.mapToJson(map));
                } else {
                    LOG.error(String.format("[%s]: map<%s>, size<%s>, message<%s>", "parseMetadata", map,
                            map.size(), "消息映射为空."));
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        try {
            String redisKey = getOutRedisKey(metadataType);
            if (!StringUtil.isBlank(redisKey) && !dataList.isEmpty()) {
                metadataRedisCache.pipRPush(redisKey, dataList);
            }
            LOG.debug(String.format("[%s]: dataList<%s>, size<%s>, fileName<%s>, redisKey<%s>, metadataType<%s>",
                    "parseMetadata", dataList, dataList.size(), fileName, redisKey, metadataType));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 获取Redis Key
     * @param msgType
     * @return
     */
    private static String getOutRedisKey(String msgType) {
        String redisKey = metadataRedisKeyMap.get(msgType);
        if (!StringUtil.isBlank(redisKey) && !StringUtil.isBlank(SystemConstant.REDIS_KEY_PREFIX)) {
            redisKey = SystemConstant.REDIS_KEY_PREFIX + ":" + redisKey;
        }
        return redisKey;
    }

    /**
     * 类型转换
     * @param value
     * @param type
     * @return
     */
    private static Object typpeConvert(String value, String type) {
        Object object = null;
        if (!StringUtil.isBlank(value)) {
            value = value.replaceAll("%%%", "|");
            switch (type) {
                case "String":
                    object = value;
                    break;
                case "int":
                    object = value.startsWith("0x") ? Integer.valueOf(value.replace("0x", "")) :
                            Integer.valueOf(value);
                    break;
                case "long":
                    object = Long.valueOf(value);
                    break;
                default:
                    object = value;
                    break;
            }
        }
        return object;
    }

    /**
     * 追加未映射字段
     * @param unMappedFieldMap
     * @param map
     */
    private static void addUnMappedField(Map<String, String> unMappedFieldMap, Map<String, Object> map) {
        if (null != unMappedFieldMap && !unMappedFieldMap.isEmpty()) {
            for (Map.Entry<String, String> en : unMappedFieldMap.entrySet()) {
                map.put(en.getKey(), null);
            }
        }
    }

    public static void main(String[] args) {
        String log = "0x01|0x01|0x0300|33445566|1542261979|9000|192.168.1.1|360|1.1.1.2|2.1.1.2|49181|23|smtp|16|smtp 1.0|501fabad96ee8f2b20888977e49e92a08bf698b6|0x5BED0CD6000A3D01|01.zip|8342|application/zip|95b15399e13e8358741f397d0bcf863fe8fdbc4c|22dabb612eb69ec4a74ae7b8df4bc08c|1|01.vir|4d0020026c8e49aaa20026fa898892ec|1|1|/Output_msg/Sample_reduction_file/0x5BED0CD6000A3D01_22dabb612eb69ec4a74ae7b8df4bc08c";
        List<String> lines = new ArrayList<>();
        lines.add(log);
        parseMetadata(lines, "udp_5.tar.gz");
    }
}
