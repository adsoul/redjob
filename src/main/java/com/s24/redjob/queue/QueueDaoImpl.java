package com.s24.redjob.queue;

import static java.util.function.Function.identity;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.ExecutionRedisSerializer;

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
   public static final String JOBS = "jobs";

   /**
    * Redis key part for the list of all job ids of a queue.
    */
   public static final String INFLIGHT = "inflight";

   /**
    * Redis serializer for job executions.
    */
   private ExecutionRedisSerializer executions;

   /**
    * Redis access.
    */
   private RedisTemplate<String, String> redis;

   @Override
   @PostConstruct
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      Assert.notNull(executions, "Precondition violated: executions != null.");

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(strings);
      redis.setValueSerializer(strings);
      redis.setHashKeySerializer(strings);
      redis.setHashValueSerializer(executions);
      redis.afterPropertiesSet();
   }

   //
   // Client related.
   //

   @Override
   public Execution enqueue(String queue, Object job, boolean front) {
      return redis.execute((RedisConnection connection) -> {
         Long id = connection.incr(key(ID));
         Execution execution = new Execution(id, job);
         connection.sAdd(key(QUEUES), value(queue));
         byte[] idBytes = value(id);
         byte[] executionBytes = executions.serialize(execution);
         if (log.isDebugEnabled()) {
            log.debug("Enqueuing: {}", new String(executionBytes, StandardCharsets.UTF_8));
         }
         connection.hSet(key(JOBS), idBytes, executionBytes);
         if (front) {
            connection.lPush(key(QUEUE, queue), idBytes);
         } else {
            connection.rPush(key(QUEUE, queue), idBytes);
         }

         return execution;
      });
   }

   @Override
   public boolean dequeue(String queue, long id) {
      return redis.execute((RedisConnection connection) -> {
         byte[] idBytes = value(id);
         Long deletes = connection.lRem(key(QUEUE, queue), 0, idBytes);
         connection.hDel(key(JOBS), idBytes);
         return deletes != null && deletes > 0;
      });
   }

   @Override
   public Execution get(long id) {
      return redis.execute((RedisConnection connection) -> {
         byte[] idBytes = value(id);
         byte[] jobBytes = connection.hGet(key(JOBS), idBytes);
         if (jobBytes == null) {
            return null;
         }

         return executions.deserialize(jobBytes);
      });
   }

   @Override
   public void update(Execution execution) {
      redis.execute((RedisConnection connection) -> {
         byte[] idBytes = value(execution.getId());
         byte[] jobBytes = executions.serialize(execution);
         boolean created = connection.hSet(key(JOBS), idBytes, jobBytes);
         if (created) {
            // Job had been deleted before, so updates are not useful, because they will create a stale job.
            connection.hDel(key(JOBS), idBytes);
         }

         return null;
      });
   }

   @Override
   public Map<Long, Execution> getAll() {
      return redis.execute((RedisConnection connection) -> {
         Map<byte[], byte[]> jobBytes = connection.hGetAll(key(JOBS));
         if (jobBytes == null) {
            return null;
         }

         return jobBytes.values().stream()
               .map(executions::deserialize)
               .collect(Collectors.toMap(Execution::getId, identity()));
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

         byte[] jobBytes = connection.hGet(key(JOBS), idBytes);
         if (jobBytes == null) {
            return null;
         }

         return executions.deserialize(jobBytes);
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
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return executions;
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      this.executions = executions;
   }
}
