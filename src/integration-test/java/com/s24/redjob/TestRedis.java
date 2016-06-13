package com.s24.redjob;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Creates connection factories for the integration test Redis.
 */
public class TestRedis {
   /**
    * Connection factory for the integration test Redis.
    * Flushes the Redis database before returning the connection factory.
    */
   public static RedisConnectionFactory connectionFactory() {
      JedisPoolConfig pool = new JedisPoolConfig();
      pool.setMaxTotal(100);

      JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
      connectionFactory.setHostName("localhost");
      connectionFactory.setPort(16379);
      connectionFactory.setDatabase(0);
      connectionFactory.setPoolConfig(pool);
      connectionFactory.afterPropertiesSet();

      flushDb(connectionFactory);

      return connectionFactory;
   }

   /**
    * Remove all keys from the given Redis database.
    */
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
