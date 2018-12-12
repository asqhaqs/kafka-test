package cn.situation.cons;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import cn.situation.util.LogUtil;
import cn.situation.util.StringUtil;

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

    public static final String SPRING_APPLICATION_CONTEXT = "applicationContext.xml";

    public static final String GEO_DATA_PATH = getProperty("geo.data.path", "geoipdata");

    public static final String ZMQ_SNDHWM = getProperty("zmq.sndhwm", "100");
    public static final String ZMQ_RCVHWM = getProperty("zmq.rcvhwm", "100");

    public static final String WORKER_THREAD_NUM = getProperty("worker.thread.num", "10");

    public static final String EXCEPT_INTERVAL_MS = getProperty("except.interval.ms", "");

    public static final String REDIS_KEY_PREFIX = getProperty("redis.key.prefix", "logcenter");
    public static final String REDIS_ALERT_KEY = getProperty("alert.redis.key", "situation-ids");

    //sftp 配置  SFTP01默认为天堤SFTP服务器
    public static final String SFTP_HOST = getProperty("sftp.host", "");
    public static final String SFTP_PORT = getProperty("sftp.port", "22");
    public static final String SFTP_USERNAME = getProperty("sftp.username", "");
    public static final String SFTP_PASSWORD = getProperty("sftp.password", "");
    public static final String SFTP_FILE_NO_CHANGE_INTERVAL = getProperty("sftp.file.nochange.interval", "0");

    public static final String IF_DOWNLOAD_METADATA = getProperty("if.download.metadata", "1");
    public static final String IF_DOWNLOAD_EVENT = getProperty("if.download.event", "1");
    public static final String IF_DOWNLOAD_ASSET = getProperty("if.download.asset", "1");

    public static final String EVENT_DIR = getProperty("event.dir", "");
    public static final String METAdDATA_DIR = getProperty("metadata.dir", "");
    public static final String ASSET_DIR = getProperty("asset.dir", "");

    public static final String PACKAGE_SUFFIX = getProperty("package.suffix", "");

    public static final String EXEC_INTERVAL_MS = getProperty("exec.interval.ms", "1000");

    public static final String LOCAL_FILE_DIR = getProperty("local.file.dir", "");

    public static final String SQLITE_DB_PATH = getProperty("sqlite.db.path", "");

    public static final String KIND_EVENT = getProperty("kind.event", "");
    public static final String KIND_METADATA = getProperty("kind.metadata", "");
    public static final String KIND_ASSET = getProperty("kind.asset", "");

    public static final String TYPE_EVENT = getProperty("type.event", "");
    public static final String TYPE_METADATA = getProperty("type.metadata", "");
    public static final String TYPE_ASSET = getProperty("type.asset", "");

    public static final String EVENT_PREFIX = getProperty("event.prefix", "");
    public static final String ASSET_PREFIX = getProperty("asset.prefix", "");

    public static final String MESSAGE_TYPE = getProperty("message.type", "");

    public static final String MESSAGE_HEAD_FIELD = getProperty("message.head.field", "");

    public static final String METADATA_REDIS_KEY = getProperty("metadata.redis.key", "");

    public static final String POSTGRESQL_URL = getProperty("postgresql.url", "");
    public static final String POSTGRESQL_USERNAME = getProperty("postgresql.username", "");
    public static final String POSTGRESQL_PASSWORD = getProperty("postgresql.password", "");
    public static final String DB_MINIDLE = getProperty("db.minidle", "8");
    public static final String DB_MAXIDLE = getProperty("db.maxidle", "16");
    public static final String DB_MAXTOTAL = getProperty("db.maxtotal", "64");
    public static final String DB_MAXWAIT_SECONDS = getProperty("db.maxwaitseconds", "180");

    public static final String MONITOR_STATISTIC_ENABLED = getProperty("monitor.statistic.enabled", "0");

    public static final String SQLITE_UPDATE_ENABLED = getProperty("sqlite.update.enabled", "0");

    public static final String INPUT_BUFFER_SIZE = getProperty("input.buffer.size", "");

    public static final Map<String, Integer> MONITOR_STATISTIC = new ConcurrentHashMap<>();

    public static final String SCHEDULE_CORE_POOL_SIZE = getProperty("schedule.core.pool.size", "1");

    public static final String MONITOR_PERIOD_SECONDS = getProperty("monitor.period.seconds", "300");

    public static final String QUEUE_CHECK_INTERVAL_MS = getProperty("queue.check.interval.ms", "10000");
    public static final String QUEUE_CHECK_INTERVAL_SIZE = getProperty("queue.check.interval.size", "1000");
    public static final String QUEUE_CHECK_POLICY = getProperty("queue.check.policy", "0");

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
