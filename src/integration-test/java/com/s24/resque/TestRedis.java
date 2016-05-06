package com.s24.resque;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import redis.clients.jedis.JedisShardInfo;

/**
 * Creates connection factories for the integration test Redis.
 */
public class TestRedis {
    /**
     * Connection factory for the integration test Redis.
     */
    public static RedisConnectionFactory connectionFactory() {
        JedisShardInfo shard = new JedisShardInfo("localhost", 16379);
        return new JedisConnectionFactory(shard);
    }
}
