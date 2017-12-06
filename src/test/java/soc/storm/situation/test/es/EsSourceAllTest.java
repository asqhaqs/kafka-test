
package soc.storm.situation.test.es;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import soc.storm.situation.utils.EsUtil;

/**
 * 
 * @author wangbin03
 *
 */
public class EsSourceAllTest {

    private final static Map<String, Object> entity = new HashMap<String, Object>();
    private final static Map<Integer, Map<String, Object>> entityCache = new HashMap<Integer, Map<String, Object>>();
    static {
        entity.put("id", 123456L);
        entity.put("name", "zhongsanmu");
        entity.put("published", new Date());

        for (int i = 0; i < 1000; i++) {
            entityCache.put(i, entity);
        }
    }

    /**
     * testDefault
     */
    public static void testDefault() {
        EsUtil.flushToEs("test_default", "test_default", entityCache, true);
    }

    /**
     * testAll
     */
    public static void testAll() {
        EsUtil.flushToEs("test_all", "test_all", entityCache, true);
    }

    /**
     * testSource
     */
    public static void testSource() {
        EsUtil.flushToEs("test_source", "test_source", entityCache, true);
    }

    public static void main(String[] args) {

        for (int j = 0; j < 10; j++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        testDefault();// es.10.187.101.154.1
                        testAll();// es.10.187.101.154.6
                        testSource();// es.10.187.101.154.3
                    }
                }
            }).start();
        }

    }

}
