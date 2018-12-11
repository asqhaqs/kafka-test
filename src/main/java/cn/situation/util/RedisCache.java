
package cn.situation.util;

import com.sun.istack.internal.Nullable;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
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

    public long rpush(K key, V value) {
        long num = redisTemplate.opsForList().rightPush(key, value);
        LOG.debug(String.format("[%s]: key<%s>, value<%s>, position<%s>", "rpush", key, value, num));
        return num;
    }

    public void rpushList(K key, List<V> dataList) {
        if (null != dataList && !dataList.isEmpty()) {
            ListOperations<K, V> listOperation = redisTemplate.opsForList();
            long num = listOperation.rightPushAll(key, (V[]) dataList.toArray());
            LOG.debug(String.format("[%s]: key<%s>, value<%s>, position<%s>", "rpushList", key, dataList, num));
        }
    }

    public void pipRPush(String key, List<String> dataList) {
        if (null != dataList && !dataList.isEmpty()) {
            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Nullable
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    try {
                        connection.openPipeline();
                        for (String data : dataList) {
                            connection.rPush(key.getBytes(), data.getBytes());
                        }
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    } finally {
                        connection.closePipeline();
                    }
                    return null;
                }
            });
        }
    }



}
