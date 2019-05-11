package com.adsoul.redjob;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Creates connection factories for the integration test Redis.
 */
public class TestRedis {
   /**
    * Connection factory for the integration test Redis.
    * Flushes the Redis database before returning the connection factory.
    */
   public static RedisConnectionFactory connectionFactory() {
      RedisStandaloneConfiguration redis = new RedisStandaloneConfiguration("localhost", 16379);
      redis.setDatabase(0);
      JedisConnectionFactory connectionFactory = new JedisConnectionFactory(redis);
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
