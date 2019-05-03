package cn.situation.util;

import org.slf4j.Logger;
import org.springframework.data.redis.core.*;

import java.util.List;

public final class RedisCache<K, V> {

    private static final Logger LOG = LogUtil.getInstance(RedisCache.class);

    private RedisTemplate<K, V> redisTemplate;

    public RedisTemplate<K, V> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<K, V> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void rpush(K key, V value) {
        try {
            redisTemplate.opsForList().rightPush(key, value);
            LOG.debug(String.format("[%s]: key<%s>, value<%s>", "rpush", key, value));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public boolean rpushList(K key, List<V> dataList) {
        boolean result = false;
        try {
            if (null != dataList && !dataList.isEmpty()) {
                ListOperations<K, V> listOperation = redisTemplate.opsForList();
                listOperation.rightPushAll(key, (V[]) dataList.toArray());
                LOG.debug(String.format("[%s]: key<%s>, value<%s>", "rpushList", key, dataList));
            }
            result = true;
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "rpushList", e.getMessage()));
        }
        return result;
    }
}
