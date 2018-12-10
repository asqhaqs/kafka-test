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
    private static volatile JedisPool assetPool = null;

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
            config.setTestOnBorrow(false);
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
            config.setTestOnBorrow(false);
            int timeout = Integer.valueOf(SystemConstant.REDIS_TIMEOUT);
            eventPool = new JedisPool(config, SystemConstant.EVENT_REDIS_HOST,
                    Integer.parseInt(SystemConstant.EVENT_REDIS_PORT), timeout);
        }
    }

    private static void initAssetPool() {
        if (null == assetPool || assetPool.isClosed()) {
            JedisPoolConfig config = new JedisPoolConfig();
            int poolMaxTotal = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_TOTAL);
            int poolMaxIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MAX_IDLE);
            int poolMinIdle = Integer.valueOf(SystemConstant.REDIS_POOL_MIN_IDLE);
            long poolMaxWait = Long.valueOf(SystemConstant.REDIS_POOL_MAX_WAIT);
            config.setMaxTotal(poolMaxTotal);
            config.setMaxIdle(poolMaxIdle);
            config.setMinIdle(poolMinIdle);
            config.setMaxWaitMillis(poolMaxWait);
            config.setTestOnBorrow(false);
            int timeout = Integer.valueOf(SystemConstant.REDIS_TIMEOUT);
            assetPool = new JedisPool(config, SystemConstant.ASSET_REDIS_HOST,
                    Integer.parseInt(SystemConstant.ASSET_REDIS_PORT), timeout);
        }
    }

    private static void closeMetadataPool() {
        if (null != metadataPool && !metadataPool.isClosed()) {
            metadataPool.close();
        }
    }

    private static void closeEventPool() {
        if (null != eventPool && !eventPool.isClosed()) {
            eventPool.close();
        }
    }

    private static void closeAssetPool() {
        if (null != assetPool && !assetPool.isClosed()) {
            assetPool.close();
        }
    }

    public static void closePool() {
        closeMetadataPool();
        closeEventPool();
        closeAssetPool();
    }

    public static void rpush(String dicName,String value, String kind) throws Exception {
        Jedis jedis = null;
        if (SystemConstant.KIND_METADATA.equals(kind)) {
            initMetadataPool();
            jedis = metadataPool.getResource();
        }
        if (SystemConstant.KIND_EVENT.equals(kind)) {
            initEventPool();
            jedis = eventPool.getResource();
        }
        if (SystemConstant.KIND_ASSET.equals(kind)) {
            initAssetPool();
            jedis = assetPool.getResource();
        }
        if (null != jedis) {
            dicName = StringUtils.trim(dicName);
            value = StringUtils.trim(value);
            jedis.rpush(dicName, value);
        }
        LOG.debug(String.format("[%s]: dicName<%s>, value<%s>, kind<%s>", "rpush", dicName, value, kind));
        if (null != jedis) {
            jedis.close();
        }
    }

    public static void main(String[] args) {

    }

}
