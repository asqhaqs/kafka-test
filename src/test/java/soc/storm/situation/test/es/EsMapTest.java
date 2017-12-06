
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
public class EsMapTest {
    //
    private final static Map<String, Object> planishMapEntity = new HashMap<String, Object>();
    private final static Map<Integer, Map<String, Object>> planishMapCache = new HashMap<Integer, Map<String, Object>>();

    //
    private final static Map<String, Object> defaultMapEntity = new HashMap<String, Object>();
    private final static Map<Integer, Map<String, Object>> defaultMapCache = new HashMap<Integer, Map<String, Object>>();

    //
    static {
        // （1）
        planishMapEntity.put("id", 123456L);
        planishMapEntity.put("name", "zhongsanmu");
        planishMapEntity.put("published", new Date());
        //
        defaultMapEntity.put("element_map", planishMapEntity);

        // （2）
        for (int i = 0; i < 1000; i++) {
            planishMapCache.put(i, planishMapEntity);
            //
            defaultMapCache.put(i, defaultMapEntity);
        }
    }

    /**
     * testPlanishMap
     */
    public static void testPlanishMap() {
        EsUtil.flushToEs("test_planish_map", "test_planish_map", planishMapCache, true);
    }

    /**
     * testDefaultMap
     */
    public static void testDefaultMap() {
        EsUtil.flushToEs("test_default_map", "test_default_map", defaultMapCache, true);
    }

    public static void main(String[] args) {

        for (int j = 0; j < 100; j++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        testDefaultMap();// es.10.187.101.154.1
                        testPlanishMap();// es.10.187.101.154.6
                    }
                }
            }).start();
        }

    }

}
