package cn.situation.cons;

import cn.situation.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

    public static final String GEO_DATA_PATH = getProperty("geo_data_path", "geoipdata");

    public static final String ZMQ_SNDHWM = getProperty("zmq_sndhwm", "100");
    public static final String ZMQ_RCVHWM = getProperty("zmq_rcvhwm", "100");

    public static final String WORKER_THREAD_NUM = getProperty("worker_thread_num", "10");

    public static final String EXCEPT_INTERVAL_MS = getProperty("except.interval.ms", "");

    // redis 配置
    public static final String REDIS_HOST = getProperty("redis.host", "127.0.0.1");
    public static final String REDIS_PORT = getProperty("redis.port", "6379");
    public static final String REDIS_TIMEOUT = getProperty("redis.timeout", "3");
    public static final String REDIS_PASSWORD = getProperty("redis.password", "123456");
    public static final String REDIS_POOL_MAX_TOTAL = getProperty("redis.poolMaxTotal", "300");
    public static final String REDIS_POOL_MAX_IDLE = getProperty("redis.poolMaxIdle", "60");
    public static final String REDIS_POOL_MIN_IDLE = getProperty("redis.poolMinIdle", "30");
    public static final String REDIS_POOL_MAX_WAIT = getProperty("redis.poolMaxWait", "5000");
    public static final String REDIS_KEY_PREFIX = getProperty("redis_key_prefix", "logcenter");
    public static final String REDIS_ALERT_KEY = getProperty("alert_redis_key", "situation-ids");

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

}
