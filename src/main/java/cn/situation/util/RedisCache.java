
package cn.situation.util;

import cn.situation.cons.SystemConstant;
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

    public void rpush(K key, V value) {
        try {
            redisTemplate.opsForList().rightPush(key, value);
            LOG.debug(String.format("[%s]: key<%s>, value<%s>", "rpush", key, value));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void rpushList(K key, List<V> dataList) {
        while (true) {
            try {
                if (null != dataList && !dataList.isEmpty()) {
                    ListOperations<K, V> listOperation = redisTemplate.opsForList();
                    listOperation.rightPushAll(key, (V[]) dataList.toArray());
                    LOG.debug(String.format("[%s]: key<%s>, value<%s>", "rpushList", key, dataList));
                }
            } catch (Exception e) {
                LOG.error(String.format("[%s]: message<%s>", "rpushList", e.getMessage()));
                if (SystemConstant.REDIS_OOM_MESSAGE.equals(e.getMessage())) {
                    try {
                        Thread.sleep(Long.parseLong(SystemConstant.REDIS_OOM_SLEEP_MS));
                    } catch (InterruptedException ie) {
                        LOG.error(ie.getMessage(), ie);
                    }
                } else {
                    break;
                }
            }
        }
    }

    public void pipRPush(String key, List<String> dataList) {
        try {
            if (null != dataList && !dataList.isEmpty()) {
                redisTemplate.executePipelined(new RedisCallback<Object>() {
                    @Nullable
                    @Override
                    public Object doInRedis(RedisConnection connection) throws DataAccessException {
                        connection.openPipeline();
                        for (String data : dataList) {
                            if (!StringUtil.isBlank(data)) {
                                connection.rPush(key.getBytes(), data.getBytes());
                            }
                        }
                        connection.openPipeline();
                        return null;
                    }
                });
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }



}
