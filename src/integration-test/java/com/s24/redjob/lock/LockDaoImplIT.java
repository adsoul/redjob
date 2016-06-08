package com.s24.redjob.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.s24.redjob.TestRedis;

/**
 * Integration test for {@link LockDaoImpl}.
 */
public class LockDaoImplIT {
   /**
    * DAO under test.
    */
   private LockDaoImpl dao = new LockDaoImpl();

   /**
    * Redis access.
    */
   private StringRedisTemplate redis;

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();

      dao.setConnectionFactory(connectionFactory);
      dao.setNamespace("namespace");
      dao.afterPropertiesSet();

      redis = new StringRedisTemplate();
      redis.setConnectionFactory(connectionFactory);
      redis.afterPropertiesSet();

      TestRedis.flushDb(connectionFactory);
   }

   @Test
   public void tryLock() {
      String key = "namespace:lock:test";

      assertTrue(dao.tryLock("test", "holder", 10, TimeUnit.SECONDS));

      assertEquals("holder", redis.opsForValue().get(key));
      assertTrue(redis.getExpire(key, TimeUnit.MILLISECONDS) > 9000);
      assertTrue(redis.getExpire(key, TimeUnit.MILLISECONDS) <= 10000);

      assertFalse(dao.tryLock("test", "someoneelse", 10, TimeUnit.SECONDS));
   }

   @Test
   public void releaseLock() {
      String key = "namespace:lock:test";

      assertTrue(dao.tryLock("test", "holder", 10, TimeUnit.SECONDS));

      dao.releaseLock("test", "holder");
      assertFalse(redis.hasKey(key));

      // After releasing the lock someone else should be able to acquire the lock.
      assertTrue(dao.tryLock("test", "someoneelse", 10, TimeUnit.SECONDS));
   }
}

