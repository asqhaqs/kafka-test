
package cn.situation.util;

import org.slf4j.Logger;
import org.springframework.data.redis.core.*;

public final class RedisCache<K, V> {

    private static final Logger LOG = LogUtil.getInstance(RedisCache.class);

    private RedisTemplate<K, V> redisTemplate;

    public RedisTemplate<K, V> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<K, V> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public long rpush(K key, V value) {
        long num = redisTemplate.opsForList().rightPush(key, value);
        LOG.debug(String.format("[%s]: key<%s>, value<%s>, num<%s>", "rpush", key, value, num));
        return num;
    }

}
