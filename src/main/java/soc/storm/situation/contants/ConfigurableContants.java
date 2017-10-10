
package soc.storm.situation.contants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author wangbin03
 *
 */
public class ConfigurableContants {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableContants.class);

    protected static Properties props = new Properties();

    protected static void init(String propertyFileName) {
        InputStream in = null;
        try {
            in = ConfigurableContants.class.getResourceAsStream(propertyFileName);
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            logger.error("[init] error: initload {} into Contants error", propertyFileName);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("[init] error: {}", e.getMessage(), e);
                }
            }
        }
    }

    protected static String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
}
