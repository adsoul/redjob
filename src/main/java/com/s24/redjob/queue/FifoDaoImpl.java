package com.s24.redjob.queue;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.json.ExecutionRedisSerializer;

/**
 * Default implementation of {@link FifoDao}.
 */
public class FifoDaoImpl extends AbstractDao implements FifoDao {
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
         byte[] executionBytes = value(execution);
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
         byte[] executionBytes = connection.hGet(key(JOBS), idBytes);
         if (executionBytes == null) {
            return null;
         }

         return parseExecution(executionBytes);
      });
   }

   @Override
   public void update(Execution execution) {
      redis.execute((RedisConnection connection) -> {
         byte[] idBytes = value(execution.getId());
         byte[] executionBytes = value(execution);
         boolean created = connection.hSet(key(JOBS), idBytes, executionBytes);
         if (created) {
            // Job had been deleted before, so updates are not useful, because they will create a stale job.
            connection.hDel(key(JOBS), idBytes);
         }

         return null;
      });
   }

   @Override
   public List<Execution> getQueued(String queue) {
      return redis.execute((RedisConnection connection) -> {
         // Get all ids from queue.
         List<byte[]> idsBytes = connection.lRange(key(QUEUE, queue), 0, -1);
         if (CollectionUtils.isEmpty(idsBytes)) {
            return emptyList();
         }

         // Lookup all executions for all ids at once.
         List<byte[]> executionsBytes = connection.hMGet(key(JOBS), idsBytes.toArray(new byte[idsBytes.size()][]));
         if (CollectionUtils.isEmpty(executionsBytes)) {
            return emptyList();
         }
         Assert.isTrue(executionsBytes.size() == idsBytes.size(),
               "Precondition violated: Redis response has the expected length.");

         return executionsBytes.stream()
               .map(this::parseExecution)
               .filter(Objects::nonNull)
               .collect(toList());
      });
   }

   @Override
   public List<Execution> getAll() {
      return redis.execute((RedisConnection connection) -> {
         Map<byte[], byte[]> executionsBytes = connection.hGetAll(key(JOBS));
         if (CollectionUtils.isEmpty(executionsBytes)) {
            return emptyList();
         }

         return executionsBytes.values().stream()
               .map(this::parseExecution)
               .filter(Objects::nonNull)
               .collect(toList());
      });
   }

   @Override
   public int cleanUp() {
      return redis.execute((RedisConnection connection) -> {
         Map<byte[], byte[]> executionsBytes = connection.hGetAll(key(JOBS));
         if (CollectionUtils.isEmpty(executionsBytes)) {
            return 0;
         }

         byte[][] toDelete = executionsBytes.entrySet().stream()
               .filter(entry -> tryParseExecution(entry.getValue()) == null)
               .map(Entry::getKey)
               .toArray(byte[][]::new);

         connection.hDel(key(JOBS), toDelete);
         return toDelete.length;
      });
   }

   /**
    * Failsafe parsing of job execution.
    *
    * @return Parsed execution or null, if deserialization fails.
    */
   private Execution tryParseExecution(byte[] executionBytes) {
      try {
         return parseExecution(executionBytes);
      } catch (Exception e) {
         return null;
      }
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

         byte[] executionBytes = connection.hGet(key(JOBS), idBytes);
         if (executionBytes == null) {
            return null;
         }

         return parseExecution(executionBytes);
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
   // Serialization.
   //

   /**
    * Serialize execution.
    *
    * @param execution
    *           Execution.
    * @return Serialized execution.
    */
   protected byte[] value(Execution execution) {
      return executions.serialize(execution);
   }

   //
   // Deserialization.
   //

   /**
    * Deserialize long value.
    *
    * @param executionBytes
    *           Execution.
    * @return Deserialized execution.
    */
   protected Execution parseExecution(byte[] executionBytes) {
      return executions.deserialize(executionBytes);
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
