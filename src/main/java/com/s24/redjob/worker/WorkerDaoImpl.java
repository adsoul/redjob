package com.s24.redjob.worker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.util.Assert;

import com.s24.redjob.AbstractDao;

/**
 * Default implementation of {@link WorkerDao}.
 */
public class WorkerDaoImpl extends AbstractDao implements WorkerDao {
   /**
    * Redis key part for set of all worker names.
    */
   public static final String WORKERS = "workers";

   /**
    * Redis key part for worker.
    */
   public static final String WORKER = "worker";

   /**
    * Redis key part for worker state.
    */
   public static final String STATE = "state";

   /**
    * Redis key part for worker stats.
    */
   public static final String STAT = "stat";

   /**
    * Redis key part for number of processed jobs.
    */
   public static final String PROCESSED = "processed";

   /**
    * Redis key part for number of failed jobs.
    */
   public static final String FAILED = "failed";

   /**
    * Redis access.
    */
   private RedisTemplate<String, String> redis;

   /**
    * JSNO serializer for {@link WorkerState}.
    */
   private final Jackson2JsonRedisSerializer<WorkerState> workerStateSerializer =
         new Jackson2JsonRedisSerializer<>(WorkerState.class);

   @Override
   @PostConstruct
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(strings);
      redis.setValueSerializer(strings);
      redis.afterPropertiesSet();
   }

   @Override
   public void ping() {
      redis.execute((RedisConnection connection) -> {
         if (!"PONG".equals(connection.ping())) {
            throw new RedisConnectionFailureException("Ping failed.");
         }
         return null;
      });
   }

   @Override
   public void state(String name, WorkerState state) {
      Assert.notNull(name, "Precondition violated: name != null.");
      Assert.notNull(state, "Precondition violated: state != null.");

      redis.execute((RedisConnection connection) -> {
         connection.sAdd(key(WORKERS), value(name));
         connection.set(key(WORKER, name, STATE), workerStateSerializer.serialize(state));
         return null;
      });
   }

   @Override
   public void stop(String name) {
      Assert.notNull(name, "Precondition violated: name != null.");

      redis.execute((RedisConnection connection) -> {
         connection.sRem(key(WORKERS), value(name));
         connection.del(
               key(WORKER, name, STATE),
               key(WORKER, name),
               key(STAT, PROCESSED, name),
               key(STAT, FAILED, name));
         return null;
      });
   }

   @Override
   public void success(String name) {
      Assert.notNull(name, "Precondition violated: name != null.");

      redis.execute((RedisConnection connection) -> {
         connection.incr(key(STAT, PROCESSED));
         connection.incr(key(STAT, PROCESSED, name));
         return null;
      });
   }

   @Override
   public void failure(String name) {
      Assert.notNull(name, "Precondition violated: name != null.");

      redis.execute((RedisConnection connection) -> {
         connection.incr(key(STAT, FAILED));
         connection.incr(key(STAT, FAILED, name));
         return null;
      });
   }

   //
   // Serialization.
   //

   /**
    * Serialize timestamp.
    *
    * @param value
    *           Timestamp.
    * @return Serialized timestamp.
    */
   protected byte[] value(LocalDateTime value) {
      return value(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
   }
}
