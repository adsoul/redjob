package com.s24.redjob.queue;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.AbstractDao;

/**
 * Default implementation of {@link QueueDao}.
 */
public class QueueDaoImpl extends AbstractDao implements QueueDao {
   /**
    * Redis key part for id sequence.
    */
   public static final String ID = "id";

   /**
    * Redis key part for set of all queue names.
    */
   public static final String QUEUES = "queues";

   /**
    * Redis key part for the list of all job ids of a queue.
    */
   public static final String QUEUE = "queue";

   /**
    * Redis key part for the hash of id -> job.
    */
   public static final String JOB = "job";

   /**
    * Redis key part for the list of all job ids of a queue.
    */
   public static final String INFLIGHT = "inflight";

   /**
    * JSON mapper.
    */
   private ObjectMapper json = new ObjectMapper();

   /**
    * Redis serializer for jobs.
    */
   private final Jackson2JsonRedisSerializer<Execution> jobs = new Jackson2JsonRedisSerializer<>(Execution.class);

   /**
    * Redis access.
    */
   private RedisTemplate<String, String> redis;

   @Override
   @PostConstruct
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      jobs.setObjectMapper(json);

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(strings);
      redis.setValueSerializer(strings);
      redis.setHashKeySerializer(strings);
      redis.setHashValueSerializer(jobs);
      redis.afterPropertiesSet();
   }

   //
   // Client related.
   //

   @Override
   public long enqueue(String queue, Object job, boolean front) {
      return redis.execute((RedisConnection connection) -> {
         Long id = connection.incr(key(ID));
         Execution execution = new Execution(id, job);

         connection.sAdd(key(QUEUES), value(queue));
         byte[] idBytes = value(id);
         connection.hSet(key(JOB, queue), idBytes, jobs.serialize(execution));
         if (front) {
            connection.lPush(key(QUEUE, queue), idBytes);
         } else {
            connection.rPush(key(QUEUE, queue), idBytes);
         }

         return id;
      });
   }

   @Override
   public void dequeue(String queue, long id) {
      redis.execute((RedisConnection connection) -> {
         byte[] idBytes = value(id);
         connection.lRem(key(QUEUE, queue), 0, idBytes);
         connection.hDel(key(JOB, queue), idBytes);
         return null;
      });
   }

   //
   // Worker related.
   //

   @Override
   public Execution pop(String queue, String worker) {
      return redis.execute((RedisConnection connection) -> {
         byte[] idBytes = connection.lPop(key(QUEUE, queue));
         if (idBytes == null) {
            return null;
         }
         connection.lPush(key(INFLIGHT, worker, queue), idBytes);

         byte[] jobBytes = connection.hGet(key(JOB, queue), idBytes);
         if (jobBytes == null) {
            return null;
         }

         return jobs.deserialize(jobBytes);
      });
   }

   @Override
   public void removeInflight(String queue, String worker) {
      redis.execute((RedisConnection connection) -> {
         connection.lPop(key(INFLIGHT, worker, queue));
         return null;
      });
   }

   @Override
   public void restoreInflight(String queue, String worker) {
      redis.execute((RedisConnection connection) -> {
         byte[] idBytes = connection.lPop(key(INFLIGHT, worker, queue));
         if (idBytes != null) {
            connection.lPush(key(QUEUE, queue), idBytes);
         }
         return null;
      });
   }

   //
   // Injections.
   //

   /**
    * JSON mapper.
    */
   public ObjectMapper getJson() {
      return json;
   }

   /**
    * JSON mapper.
    */
   public void setJson(ObjectMapper json) {
      this.json = json;
   }
}
