package soc.storm.situation.utils;


import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import soc.storm.situation.contants.SystemMapEnrichConstants;

/**
 * @author lenzhao
 * @date 2018/11/14 15:44
 *
 */
public class DicUtil {

    private static  final Logger LOG = LoggerFactory.getLogger(DicUtil.class);
    private static volatile JedisPool pool = null;

    private static void initPool() {
        if (null == pool || pool.isClosed()) {
            JedisPoolConfig config = new JedisPoolConfig();
            int poolMaxTotal = Integer.valueOf(SystemMapEnrichConstants.REDIS_POOL_MAX_TOTAL);
            int poolMaxIdle = Integer.valueOf(SystemMapEnrichConstants.REDIS_POOL_MAX_IDLE);
            int poolMinIdle = Integer.valueOf(SystemMapEnrichConstants.REDIS_POOL_MIN_IDLE);
            long poolMaxWait = Long.valueOf(SystemMapEnrichConstants.REDIS_POOL_MAX_WAIT);
            config.setMaxTotal(poolMaxTotal);
            config.setMaxIdle(poolMaxIdle);
            config.setMinIdle(poolMinIdle);
            config.setMaxWaitMillis(poolMaxWait);
            String host = SystemMapEnrichConstants.REDIS_HOST;
            int port = Integer.valueOf(SystemMapEnrichConstants.REDIS_PORT);
            int timeout = Integer.valueOf(SystemMapEnrichConstants.REDIS_TIMEOUT);
            pool = new JedisPool(config, host, port, timeout);
        }
    }

    public void closePool() {
        if (null != pool && !pool.isClosed()) {
            pool.close();
        }
    }

    public static void rpush(String dicName,String value) {
        Jedis jedis = null;
        try {
            initPool();
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            value = StringUtils.trim(value);
            jedis.rpush(dicName, value);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.format("[%s]: message<%s>", "rpush", e.getLocalizedMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void lpush(String dicName,String value) {
        Jedis jedis = null;
        try {
            initPool();
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            value = StringUtils.trim(value);
            jedis.lpush(dicName, value);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "lpush", e.getLocalizedMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static String rpop(String dicName) {
        Jedis jedis = null;
        String result = "";
        try {
            initPool();
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            result = jedis.rpop(dicName);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "rpop", e.getLocalizedMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

    public  static String lpop(String dicName){
        Jedis jedis = null;
        String result = "";
        try {
            initPool();
            jedis = pool.getResource();
            dicName = StringUtils.trim(dicName);
            result = jedis.lpop(dicName);
        } catch (Exception e) {
            LOG.error(String.format("[%s]: message<%s>", "lpop", e.getLocalizedMessage()));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }


//    public static void main(String[] args) {
//        int i = 0;
//        while(i< 1){
//            DicUtil.rpush("logcenter:situatiaaaaon-ids","ssss" + i++);
//        }
//
//    }

}
