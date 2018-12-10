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
        redisCacheMap.put(SystemConstant.METADATA_REDIS_CACHE, (RedisCache) ctx.getBean(SystemConstant.METADATA_REDIS_CACHE));
        redisCacheMap.put(SystemConstant.EVENT_REDIS_CACHE, (RedisCache) ctx.getBean(SystemConstant.EVENT_REDIS_CACHE));
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
