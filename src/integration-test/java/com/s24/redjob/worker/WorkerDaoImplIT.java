package com.s24.redjob.worker;

import com.s24.redjob.TestRedis;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link WorkerDaoImpl}.
 */
class WorkerDaoImplIT {
   /**
    * DAO under test.
    */
   private WorkerDaoImpl dao = new WorkerDaoImpl();

   /**
    * Redis template.
    */
   private RedisTemplate<String, String> redis;

   @BeforeEach
   void setUp() {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();
      dao.setConnectionFactory(connectionFactory);
      dao.setNamespace("namespace");
      dao.afterPropertiesSet();

      redis = new StringRedisTemplate();
      redis.setConnectionFactory(connectionFactory);
      redis.afterPropertiesSet();
   }

   @Test
   void ping() {
      dao.ping();
   }

   @Test
   void state() {
      WorkerState test = new WorkerState();
      dao.state("test", test);

      assertThat(dao.names()).containsOnly("test");
      assertNotNull(redis.opsForValue().get("namespace:worker:test:state"));
   }

   @Test
   void stop() {
      WorkerState test = new WorkerState();
      dao.state("test", test);
      dao.success("test");
      dao.failure("test");
      dao.stop("test");

      assertThat(dao.names()).isEmpty();
      assertNull(redis.opsForValue().get("namespace:worker:test:state"));
      assertNull(redis.opsForValue().get("namespace:stat:processed:test"));
      assertNull(redis.opsForValue().get("namespace:stat:failed:test"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:processed"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:failed"));
   }

   @Test
   void success() {
      dao.success("test1");
      dao.success("test2");

      assertEquals("2", redis.opsForValue().get("namespace:stat:processed"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:processed:test1"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:processed:test2"));
      assertNull(redis.opsForValue().get("namespace:stat:failed"));
   }

   @Test
   void failure() {
      dao.failure("test1");
      dao.failure("test2");

      assertEquals("2", redis.opsForValue().get("namespace:stat:failed"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:failed:test1"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:failed:test2"));
      assertNull(redis.opsForValue().get("namespace:stat:processed"));
   }

   @Test
   void names() {
      WorkerState test1 = new WorkerState();
      dao.state("test1", test1);
      WorkerState test2 = new WorkerState();
      dao.state("test2", test2);

      assertThat(dao.names()).containsOnly("test1", "test2");
   }
}
