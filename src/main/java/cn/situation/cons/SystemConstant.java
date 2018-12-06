package cn.situation.cons;

import cn.situation.util.LogUtil;
import cn.situation.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class SystemConstant {

    private static final Logger LOG = LogUtil.getInstance(SystemConstant.class);

    private static Properties props = new Properties();

    private static void init(String fileName) {
        InputStream in = null;
        try {
            in = ClassLoader.getSystemResourceAsStream(fileName);
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            props.load(inputStreamReader);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    private static String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
    static {
        LOG.info("init properties");
        init("app.properties");
    }

    public static final String GEO_DATA_PATH = getProperty("geo.data.path", "geoipdata");

    public static final String ZMQ_SNDHWM = getProperty("zmq.sndhwm", "100");
    public static final String ZMQ_RCVHWM = getProperty("zmq.rcvhwm", "100");

    public static final String WORKER_THREAD_NUM = getProperty("worker.thread.num", "10");

    public static final String EXCEPT_INTERVAL_MS = getProperty("except.interval.ms", "");

    // redis 配置
    public static final String METADATA_REDIS_HOST = getProperty("metadata.redis.host", "127.0.0.1");
    public static final String METADATA_REDIS_PORT = getProperty("metadata.redis.port", "6379");
    public static final String METADATA_REDIS_PASSWORD = getProperty("metadata.redis.password", "123456");

    public static final String EVENT_REDIS_HOST = getProperty("event.redis.host", "127.0.0.1");
    public static final String EVENT_REDIS_PORT = getProperty("event.redis.port", "6379");
    public static final String EVENT_REDIS_PASSWORD = getProperty("event.redis.password", "123456");

    public static final String ASSERT_REDIS_HOST = getProperty("assert.redis.host", "127.0.0.1");
    public static final String ASSERT_REDIS_PORT = getProperty("assert.redis.port", "6379");
    public static final String ASSERT_REDIS_PASSWORD = getProperty("assert.redis.password", "123456");

    public static final String REDIS_TIMEOUT = getProperty("redis.timeout", "3");
    public static final String REDIS_POOL_MAX_TOTAL = getProperty("redis.poolMaxTotal", "300");
    public static final String REDIS_POOL_MAX_IDLE = getProperty("redis.poolMaxIdle", "60");
    public static final String REDIS_POOL_MIN_IDLE = getProperty("redis.poolMinIdle", "30");
    public static final String REDIS_POOL_MAX_WAIT = getProperty("redis.poolMaxWait", "5000");
    public static final String REDIS_KEY_PREFIX = getProperty("redis.key.prefix", "logcenter");
    public static final String REDIS_ALERT_KEY = getProperty("alert.redis.key", "situation-ids");

    //sftp 配置  SFTP01默认为天堤SFTP服务器
    public static final String SFTP_HOST = getProperty("sftp.host", "");
    public static final String SFTP_PORT = getProperty("sftp.port", "22");
    public static final String SFTP_USERNAME = getProperty("sftp.username", "");
    public static final String SFTP_PASSWORD = getProperty("sftp.password", "");

    public static final String LEVEL1_DIR = getProperty("level1.dir", "");

    public static final String IF_DOWNLOAD_METADATA = getProperty("if.download.metadata", "1");
    public static final String IF_DOWNLOAD_EVENT = getProperty("if.download.event", "1");
    public static final String IF_DOWNLOAD_ASSERT = getProperty("if.download.assert", "1");

    public static final String EVENT_DIR = getProperty("event.dir", "");
    public static final String METAdDATA_DIR = getProperty("metadata.dir", "");
    public static final String ASSERT_DIR = getProperty("assert.dir", "");

    public static final String PACKAGE_SUFFIX = getProperty("package.suffix", "");

    public static final String EXEC_INTERVAL_MS = getProperty("exec.interval.ms", "1000");

    public static final String LOCAL_FILE_DIR = getProperty("local.file.dir", "");

    public static final String SQLITE_DB_PATH = getProperty("sqlite.db.path", "");

    public static final String KIND_EVENT = getProperty("kind.event", "");
    public static final String KIND_METADATA = getProperty("kind.metadata", "");
    public static final String KIND_ASSERT = getProperty("kind.assert", "");

    public static final String TYPE_EVENT = getProperty("type.event", "");
    public static final String TYPE_METADATA = getProperty("type.metadata", "");
    public static final String TYPE_ASSERT = getProperty("type.assert", "");

    public static final String EVENT_PREFIX = getProperty("event.prefix", "");
    public static final String ASSERT_PREFIX = getProperty("assert.prefix", "");

    public static final String MESSAGE_TYPE = getProperty("message.type", "");

    public static final String MESSAGE_HEAD_FIELD = getProperty("message.head.field", "");

    public static final String METADATA_REDIS_KEY = getProperty("metadata.redis.key", "");

    public static Map<String, Map<String, String>> getMetadataFieldMap() {
        Map<String, Map<String, String>> messageFieldMap = new HashMap<>();
        String[] metadataTypes = TYPE_METADATA.split(",");
        for (String metadataType : metadataTypes) {
            Map<String, String> fieldTypeMap = new LinkedHashMap<>();
            String[] fieldTypes = getProperty("metadata." + metadataType, "").split(",");
            for (String fieldType : fieldTypes) {
                if (!StringUtil.isBlank(fieldType)) {
                    fieldTypeMap.put(fieldType.split(":")[0], fieldType.split(":")[1]);
                }
            }
            messageFieldMap.put(metadataType, fieldTypeMap);
        }
        return messageFieldMap;
    }

    public static Map<String, Map<String, String>> getMetadataUnMappedFieldMap() {
        Map<String, Map<String, String>> metadataUnMappedFieldMap = new HashMap<>();
        String[] metadataTypes = TYPE_METADATA.split(",");
        for (String metadataType : metadataTypes) {
            Map<String, String> fieldTypeMap = new HashMap<>();
            String[] fieldTypes = getProperty("metadata." + metadataType + ".unmapped", "").split(",");
            for (String fieldType : fieldTypes) {
                if (!StringUtil.isBlank(fieldType)) {
                    fieldTypeMap.put(fieldType.split(":")[0], fieldType.split(":")[1]);
                }
            }
            metadataUnMappedFieldMap.put(metadataType, fieldTypeMap);
        }
        return metadataUnMappedFieldMap;
    }

    public static Map<String, Map<String, String>> getMetadataMappedFieldMap() {
        Map<String, Map<String, String>> metadataMappedFieldMap = new HashMap<>();
        String[] metadataTypes = TYPE_METADATA.split(",");
        for (String metadataType : metadataTypes) {
            Map<String, String> fieldMap = new HashMap<>();
            String[] fieldTypes = getProperty("metadata." + metadataType + ".mapped", "").split(",");
            for (String fieldType : fieldTypes) {
                if (!StringUtil.isBlank(fieldType)) {
                    fieldMap.put(fieldType.split(":")[0], fieldType.split(":")[1]);
                }
            }
            metadataMappedFieldMap.put(metadataType, fieldMap);
        }
        return metadataMappedFieldMap;
    }

    public static Map<String, Map<String, String>> getMetadataMappedTypeMap() {
        Map<String, Map<String, String>> metadataMappedTypeMap = new HashMap<>();
        String[] metadataTypes = TYPE_METADATA.split(",");
        for (String metadataType : metadataTypes) {
            Map<String, String> fieldTypeMap = new HashMap<>();
            String[] fieldTypes = getProperty("metadata." + metadataType + ".mapped", "").split(",");
            for (String fieldType : fieldTypes) {
                if (!StringUtil.isBlank(fieldType)) {
                    fieldTypeMap.put(fieldType.split(":")[1], fieldType.split(":")[2]);
                }
            }
            metadataMappedTypeMap.put(metadataType, fieldTypeMap);
        }
        return metadataMappedTypeMap;
    }

}
