package org.pradheeparul.utils;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.JedisPooled;

/**
 * Cache
 *
 * @author PradheepKumarA
 */
public class Cache {
    private static final Logger logger = LogManager.getLogger(Cache.class);
    private static int CACHE_EXPIRE_TIME_IN_SECONDS = 24 * 60 * 60;
    private static JedisPooled cache;

    public static JedisPooled getInstance() {
        if (cache == null) {
            String host = PropertyUtils.getProperty("redis", "redis_host");
            int port = PropertyUtils.getIntegerProperty("redis", "redis_port");
            cache = new JedisPooled(host, port);
        }
        return cache;
    }

    public static void set(String key, String value) {
        try {
            getInstance().setex(key, CACHE_EXPIRE_TIME_IN_SECONDS, value);
        } catch (Exception ex) {
            logger.error("Error occurred while setting key: " + key, ex);
        }

    }

    public static String get(String key) {
        return getInstance().get(key);
    }

    public static boolean exists(String key) {
        return getInstance().exists(key);
    }

}
