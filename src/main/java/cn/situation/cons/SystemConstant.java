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
            LOG.error(e.getLocalizedMessage());
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

    //sftp 配置
    public static final String SFTP_HOST = getProperty("sftp.host", "");
    public static final String SFTP_PORT = getProperty("sftp.port", "22");
    public static final String SFTP_USERNAME = getProperty("sftp.username", "");
    public static final String SFTP_PASSWORD = getProperty("sftp.password", "");

    public static final String EXEC_INTERVAL_MS = getProperty("exec.interval.ms", "1000");

}
