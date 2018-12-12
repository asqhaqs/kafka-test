package cn.situation.util;

import cn.situation.cons.SystemConstant;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lenzhao
 * @date 2018/12/10 15:17
 * @description TODO
 */
public class RedisUtil {

    private static final Logger LOG = LogUtil.getInstance(RedisUtil.class);

    private static Map<String, RedisCache> redisCacheMap = new HashMap<>();

    static {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(SystemConstant.SPRING_APPLICATION_CONTEXT);
        redisCacheMap.put("metadata_0", (RedisCache) ctx.getBean("metadata1RedisCache"));
        redisCacheMap.put("metadata_1", (RedisCache) ctx.getBean("metadata2RedisCache"));
        redisCacheMap.put("metadata_2", (RedisCache) ctx.getBean("metadata3RedisCache"));
        redisCacheMap.put("event_0", (RedisCache) ctx.getBean("event1RedisCache"));
        redisCacheMap.put("event_1", (RedisCache) ctx.getBean("event2RedisCache"));
        ((ClassPathXmlApplicationContext) ctx).close();
    }

    private RedisUtil() {

    }

    public static RedisCache getRedisCache(String redisType) {
        RedisCache redisCache = redisCacheMap.get(redisType);
        if (null == redisCache) {
            LOG.error(String.format("[%s]: redisType<%s>, message<%s>", "getRedisCache", redisType, "redisCache is null."));
            return null;
        }
        return redisCache;
    }
}
