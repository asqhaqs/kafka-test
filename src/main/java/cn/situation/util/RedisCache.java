
package cn.situation.util;

import org.springframework.data.redis.core.*;

public final class RedisCache<K, V> {

    private RedisTemplate<K, V> redisTemplate;

    public RedisTemplate<K, V> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<K, V> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public long rpush(K key, V value) {
        return redisTemplate.opsForList().rightPush(key, value);
    }

}
