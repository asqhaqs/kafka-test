package cn.situation.cons;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import org.slf4j.Logger;
import cn.situation.util.LogUtil;

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

    public static final String GEO_DATA_PATH = getProperty("geo.data.path", "");

    public static final String KAFKA_SERVER_JAAS_FILE = "kafka_server_jaas.conf";
    public static final String KAFKA_KRB_FILE = "krb5.conf";
    public static final String IS_KERBEROS = getProperty("kafka.isKerberos", "true");

    public static final String BROKER_URL = getProperty("kafka.consumer.brokers.list", "127.0.0.1:9092");

    public static final String INPUT_BUFFER_SIZE = getProperty("input.buffer.size", "");

    public static final String TOPIC_LIST = getProperty("kafka.consumer.source.topic.list", "");

}
