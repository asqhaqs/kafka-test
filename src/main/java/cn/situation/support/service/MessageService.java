package cn.situation.support.service;

import cn.situation.cons.SystemConstant;
import cn.situation.util.DicUtil;
import cn.situation.util.JsonUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.StringUtil;
import org.slf4j.Logger;

import javax.jws.soap.SOAPBinding;
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
        if (values.length <= messageHeadFieldSize) {
            LOG.error(String.format("[%s]: line<%s>, size<%s>, headSize<%s>, message<%s>", "parseMetadata",
                    line, values.length, messageHeadFieldSize, "消息总长度应大于消息头长度."));
            return;
        }
        String msgType = values[2];
        String metadataType = messageTypeMap.get(msgType);
        LOG.info(String.format("[%s]: line<%s>, msgType<%s>, metadataType<%s>, size<%s>", "parseMetadata",
                line, msgType, metadataType, values.length));
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
        try {
            if (values.length != (messageHeadFieldSize + messageFieldList.size())) {
                LOG.error(String.format("[%s]: line<%s>, metadataType<%s>, size<%s>, msgSize<%s>, message<%s>",
                        "parseMetadata", line, metadataType, values.length,
                        (messageHeadFieldSize + messageFieldList.size()), "消息总长度与定义长度不一致."));
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
            LOG.info(String.format("[%s]: oriFieldMap<%s>, metadataType<%s>, size<%s>", "parseMetadata",
                    fieldMap, metadataType, fieldMap.size()));
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
    private void addUnMappedField(Map<String, String> unMappedFieldMap, Map<String, Object> map) {
        if (null != unMappedFieldMap && !unMappedFieldMap.isEmpty()) {
            for (Map.Entry<String, String> en : unMappedFieldMap.entrySet()) {
                map.put(en.getKey(), null);
            }
        }
    }

    public static void main(String[] args) {
        MessageService service = new MessageService();
        String log = "0x01|0x01|0x020C|305441741|1544095234|9|172.24.201.141|360|192.168.74.30|192.168.74.31|1051|502||||e179dd7caf1ce974e9a8985869b21d5fc1b30855|0x5C09060200035403|1|0|100|";
        service.parseMetadata(log);
    }
}
