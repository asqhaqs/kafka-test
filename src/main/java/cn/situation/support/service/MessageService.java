package cn.situation.support.service;

import cn.situation.cons.SystemConstant;
import cn.situation.util.DicUtil;
import cn.situation.util.JsonUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.StringUtil;
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
     * @param line
     */
    public void parseMetadata(String line) {
        String[] values = line.split("\\|", -1);
        String msgType = values[2];
        String metadataType = messageTypeMap.get(msgType);
        LOG.info(String.format("[%s]: line<%s>, msgType<%s>, metadataType<%s>, size<%s>", "parseMetadata",
                line, msgType, metadataType, values.length));
        Map<String, String> unMappedMap = metadataUnMappedFieldMap.get(metadataType);
        Map<String, String> mappedMap = metadataMappedFieldMap.get(metadataType);
        Map<String, String> mappedTypeMap = metadataMappedTypeMap.get(metadataType);
        Map<String, String> messageFieldMap = metadataFieldMap.get(metadataType);
        List<String> messageFieldList = new ArrayList<>();
        for (Map.Entry<String, String> en : messageFieldMap.entrySet()) {
            messageFieldList.add(en.getKey());
        }
        Map<String, String> fieldMap = new HashMap<>();
        Map<String, String> typeMap = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        try {
            if (values.length != (messageHeadFieldSize + messageFieldList.size())) {
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
                fieldMap.put(fieldName, values[j]);
            }
            LOG.info(String.format("[%s]: oriFieldMap<%s>, metadataType<%s>", "parseMetadata", fieldMap, metadataType));
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
            if (!map.isEmpty()) {
                String redisKey = getOutRedisKey(metadataType);
                if (!StringUtil.isBlank(redisKey)) {
                    DicUtil.rpush(redisKey, JsonUtil.mapToJson(map), SystemConstant.KIND_METADATA);
                }
                LOG.info(String.format("[%s]: jsonData<%s>, size<%s>, redisKey<%s>, metadataType<%s>", "parseMetadata",
                        JsonUtil.mapToJson(map), map.size(), redisKey, metadataType));
            } else {
                LOG.error(String.format("[%s]: map<%s>, size<%s>, message<%s>", "parseMetadata", map,
                        map.size(), "消息映射为空."));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 获取Redis Key
     * @param msgType
     * @return
     */
    private String getOutRedisKey(String msgType) {
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
    private Object typpeConvert(String value, String type) {
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
            }
        }
        return object;
    }

    /**
     * 追加未映射字段
     * @param unMappedFieldMap
     * @param map
     */
    private void addUnMappedField(Map<String, String> unMappedFieldMap, Map<String, Object> map) {
        if (null != unMappedFieldMap && !unMappedFieldMap.isEmpty()) {
            for (Map.Entry<String, String> en : unMappedFieldMap.entrySet()) {
                map.put(en.getKey(), null);
            }
        }
    }

    public static void main(String[] args) {
        MessageService service = new MessageService();
        String log = "0x01|0x01|0x0201|33445566|1542261974|47|192.168.1.1|360|1.1.1.2|2.1.1.2|49183|80|3091|||501fabad96ee8f2b20888977e49e92a08bf698b6|0x5BED0CD6000C3D01|1|1.1|0x01|www.baidu.com|/index.html|https://developer.mozilla.org/en-US/docs/Web/JavaScript|Mozilla/5.0 (X11; Linux x86_64)|Basic YWxhZGRpbjpvcGVuc2VzYW1l|Basic YWxhZGRpbjpvcGVuc2VzYW1l|PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;|/index.html|Apache/2.4.1 (Unix)|33a64df551425fcc55e4d42a148795d9f25f89d4|203.0.113.195|200|gzip|test data|Expires|deflate|HTTP/1.1 GWA|no-cache|keep-alive|gzip|de, en|/my-first-blog-post|bytes 200-1000/67589|1024|application/json; charset=utf-8|utf-8|x_flash_version|1024|bytes=200-1000|Wed, 21 Oct 2015 07:28:00 GMT|keep-alive|abc.json|gzip|application/json";
        service.parseMetadata(log);
    }
}
