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
    private static volatile JedisPool pool = null;

    private static void initPool(String host, int port) {
        if (null == pool || pool.isClosed()) {
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
            pool = new JedisPool(config, host, port, timeout);
        }
    }

    public void closePool() {
        if (null != pool && !pool.isClosed()) {
            pool.close();
        }
    }

    public static void rpush(String host, int port, String dicName,String value) {
        Jedis jedis = null;
        try {
            initPool(host, port);
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            value = StringUtils.trim(value);
            jedis.rpush(dicName, value);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.format("[%s]: message<%s>", "rpush", e.getMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void lpush(String host, int port, String dicName,String value) {
        Jedis jedis = null;
        try {
            initPool(host, port);
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            value = StringUtils.trim(value);
            jedis.lpush(dicName, value);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "lpush", e.getMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static String rpop(String host, int port, String dicName) {
        Jedis jedis = null;
        String result = "";
        try {
            initPool(host, port);
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            result = jedis.rpop(dicName);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "rpop", e.getMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

    public  static String lpop(String host, int port, String dicName){
        Jedis jedis = null;
        String result = "";
        try {
            initPool(host, port);
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            result = jedis.lpop(dicName);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "lpop", e.getMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

    public static void main(String[] args) {

    }

}
