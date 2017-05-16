package com.s24.redjob.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.s24.redjob.TestRedis;

/**
 * Integration test for {@link WorkerDaoImpl}.
 */
public class WorkerDaoImplIT {
   /**
    * DAO under test.
    */
   private WorkerDaoImpl dao = new WorkerDaoImpl();

   /**
    * Redis template.
    */
   private RedisTemplate<String, String> redis;

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();
      dao.setConnectionFactory(connectionFactory);
      dao.setNamespace("namespace");
      dao.afterPropertiesSet();

      redis = new StringRedisTemplate();
      redis.setConnectionFactory(connectionFactory);
      redis.afterPropertiesSet();
   }

   @Test
   public void ping() {
      dao.ping();
   }

   @Test
   public void state() throws Exception {
      WorkerState test = new WorkerState();
      dao.state("test", test);

      assertThat(dao.names()).containsOnly("test");
      assertNotNull(redis.opsForValue().get("namespace:worker:test:state"));
   }

   @Test
   public void stop() throws Exception {
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
   public void success() throws Exception {
      dao.success("test1");
      dao.success("test2");

      assertEquals("2", redis.opsForValue().get("namespace:stat:processed"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:processed:test1"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:processed:test2"));
      assertNull(redis.opsForValue().get("namespace:stat:failed"));
   }

   @Test
   public void failure() throws Exception {
      dao.failure("test1");
      dao.failure("test2");

      assertEquals("2", redis.opsForValue().get("namespace:stat:failed"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:failed:test1"));
      assertEquals("1", redis.opsForValue().get("namespace:stat:failed:test2"));
      assertNull(redis.opsForValue().get("namespace:stat:processed"));
   }

   @Test
   public void names() throws Exception {
      WorkerState test1 = new WorkerState();
      dao.state("test1", test1);
      WorkerState test2 = new WorkerState();
      dao.state("test2", test2);

      assertThat(dao.names()).containsOnly("test1", "test2");
   }
}
