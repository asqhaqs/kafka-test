package cn.situation.util;

import cn.situation.cons.SystemConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author lenzhao
 * @date 2018/11/14 15:44
 */
public class DicUtil {

    private static  final Logger LOG = LogUtil.getInstance(DicUtil.class);
    private static volatile JedisPool metadataPool = null;
    private static volatile JedisPool eventPool = null;
    private static volatile JedisPool assertPool = null;

    private static void initMetadataPool() {
        if (null == metadataPool || metadataPool.isClosed()) {
            JedisPoolConfig config = new JedisPoolConfig();
            int poolMaxTotal = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_TOTAL);
            int poolMaxIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_IDLE);
            int poolMinIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MIN_IDLE);
            long poolMaxWait = Long.valueOf(SystemConstant.REDIS_POOL_MAX_WAIT);
            config.setMaxTotal(poolMaxTotal);
            config.setMaxIdle(poolMaxIdle);
            config.setMinIdle(poolMinIdle);
            config.setMaxWaitMillis(poolMaxWait);
            int timeout = Integer.valueOf(SystemConstant.REDIS_TIMEOUT);
            metadataPool = new JedisPool(config, SystemConstant.METADATA_REDIS_HOST,
                    Integer.parseInt(SystemConstant.METADATA_REDIS_PORT), timeout);
        }
    }

    private static void initEventPool() {
        if (null == eventPool || eventPool.isClosed()) {
            JedisPoolConfig config = new JedisPoolConfig();
            int poolMaxTotal = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_TOTAL);
            int poolMaxIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_IDLE);
            int poolMinIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MIN_IDLE);
            long poolMaxWait = Long.valueOf(SystemConstant.REDIS_POOL_MAX_WAIT);
            config.setMaxTotal(poolMaxTotal);
            config.setMaxIdle(poolMaxIdle);
            config.setMinIdle(poolMinIdle);
            config.setMaxWaitMillis(poolMaxWait);
            int timeout = Integer.valueOf(SystemConstant.REDIS_TIMEOUT);
            eventPool = new JedisPool(config, SystemConstant.EVENT_REDIS_HOST,
                    Integer.parseInt(SystemConstant.EVENT_REDIS_PORT), timeout);
        }
    }

    private static void initAssertPool() {
        if (null == assertPool || assertPool.isClosed()) {
            JedisPoolConfig config = new JedisPoolConfig();
            int poolMaxTotal = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_TOTAL);
            int poolMaxIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_IDLE);
            int poolMinIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MIN_IDLE);
            long poolMaxWait = Long.valueOf(SystemConstant.REDIS_POOL_MAX_WAIT);
            config.setMaxTotal(poolMaxTotal);
            config.setMaxIdle(poolMaxIdle);
            config.setMinIdle(poolMinIdle);
            config.setMaxWaitMillis(poolMaxWait);
            int timeout = Integer.valueOf(SystemConstant.REDIS_TIMEOUT);
            assertPool = new JedisPool(config, SystemConstant.ASSERT_REDIS_HOST,
                    Integer.parseInt(SystemConstant.ASSERT_REDIS_PORT), timeout);
        }
    }

    private void closeMetadataPool() {
        if (null != metadataPool && !metadataPool.isClosed()) {
            metadataPool.close();
        }
    }

    private void closeEventPool() {
        if (null != eventPool && !eventPool.isClosed()) {
            eventPool.close();
        }
    }

    private void closeAssertPool() {
        if (null != assertPool && !assertPool.isClosed()) {
            assertPool.close();
        }
    }

    public static void rpush(String dicName,String value, String kind) {
        Jedis jedis = null;
        try {
            if (SystemConstant.KIND_METADATA.equals(kind)) {
                initMetadataPool();
                jedis = metadataPool.getResource();
            }
            if (SystemConstant.KIND_EVENT.equals(kind)) {
                initEventPool();
                jedis = eventPool.getResource();
            }
            if (SystemConstant.KIND_ASSERT.equals(kind)) {
                initAssertPool();
                jedis = assertPool.getResource();
            }
            if (null != jedis) {
                dicName = StringUtils.trim(dicName);
                value = StringUtils.trim(value);
                jedis.rpush(dicName, value);
            }
            LOG.info(String.format("[%s]: dicName<%s>, value<%s>, kind<%s>", "rpush", dicName, value, kind));
        } catch (Exception e) {
            LOG.error(String.format("[%s]: kind<%s>, message<%s>", "rpush", kind, e.getMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void main(String[] args) {

    }

}
