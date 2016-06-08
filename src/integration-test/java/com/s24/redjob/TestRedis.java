package com.s24.redjob;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

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

   public static void flushDb(RedisConnectionFactory connectionFactory) {
      RedisTemplate<?, ?> redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.afterPropertiesSet();

      redis.execute((RedisConnection connection) -> {
         connection.flushDb();
         return null;
      });
   }
}
