package com.s24.redjob.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
   public void tryLock_parallel() throws Exception {
      final int threads = 100;

      CompletableFuture<Void> lock = new CompletableFuture<>();
      AtomicInteger acquired = new AtomicInteger(0);
      AtomicInteger notAcquired = new AtomicInteger(0);

      ExecutorService pool = Executors.newFixedThreadPool(threads);
      for (int i = 0; i < threads; i++) {
         String holder = Integer.toString(i);
         pool.submit(() -> {
            try {
               // Warmup redis connection pool.
               redis.hasKey("dummy");

               // Wait for start.
               lock.get();

               // Try to acquire lock and log success.
               if (dao.tryLock("test", holder, 10, TimeUnit.SECONDS)) {
                  acquired.incrementAndGet();
               } else {
                  notAcquired.incrementAndGet();
               }

            } catch (Exception e) {
               fail("No exception in test threads expected.");
               System.out.println("failed");
            }
         });
      }

      // Wait at max 10 seconds for all threads to arrive at start lock.
      for (int i = 0; i < 10 && lock.getNumberOfDependents() < threads; i++) {
         Thread.sleep(1000);
      }

      // Start all threads at once.
      lock.complete(null);

      // Wait at max 10 seconds for all threads to try to acquire lock.
      for (int i = 0; i < 10 && acquired.get() + notAcquired.get() < threads; i++) {
         Thread.sleep(1000);
      }

      // Check that exactly one thread was able to acquire the lock.
      assertEquals(1, acquired.get());
      assertEquals(threads - 1, notAcquired.get());
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
