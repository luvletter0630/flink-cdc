package com.flink.demo.redis;

import redis.clients.jedis.Jedis;

/**
 * @author liwj
 * @date 2024/4/8 15:16
 * @description:
 */
public class RedisSingleton {
    private static Jedis jedis;

    private static int expireTime = 2000;

    private static final String  lockKey = "flink-es-lock-key";

    private RedisSingleton() {
    }

    public static Jedis getJedis(String hostname, int port, String password) {
        if (jedis == null) {
            synchronized (RedisSingleton.class) {
                if (jedis == null) {
                    jedis = initJedis(hostname, port, password);
                }
            }
        }
        return jedis;
    }

    public static boolean lock() {
        if (jedis == null){
            jedis = getJedis("172.30.141.239", 6379, "Zhu88jie!");
        }
        long expires = System.currentTimeMillis() + expireTime + 1;
        String expiresStr = String.valueOf(expires); // 锁到期时间

        if (jedis.setnx(lockKey, expiresStr) == 1) {
            // 获取锁成功
            return true;
        }

        String currentValueStr = jedis.get(lockKey); // Redis里面的时间
        if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {
            // 锁已经过期，获取锁并更新过期时间
            String oldValueStr = jedis.getSet(lockKey, expiresStr);
            if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
                // 更新锁的过期时间成功
                return true;
            }
        }

        // 其他情况，无法获取锁
        return false;
    }

    public static void unlock() {
        jedis.del(lockKey); // 释放锁
    }

    public static Jedis initJedis(String hostname, int port, String password) {
        Jedis jedis = new Jedis(hostname, port);
        jedis.auth(password);
        return jedis;
    }
}
