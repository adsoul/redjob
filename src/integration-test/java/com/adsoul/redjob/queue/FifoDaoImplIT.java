package com.adsoul.redjob.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.adsoul.redjob.TestRedis;
import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.json.ExecutionRedisSerializer;
import com.adsoul.redjob.worker.json.TestExecutionRedisSerializer;
import com.adsoul.redjob.worker.runner.TestJob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link FifoDaoImpl}.
 */
class FifoDaoImplIT {
   /**
    * Test queue.
    */
   private static final String QUEUE = "test-queue";

   /**
    * DAO under test.
    */
   private FifoDaoImpl dao = new FifoDaoImpl();

   @BeforeEach
   void setUp()  {
      ExecutionRedisSerializer executions = new TestExecutionRedisSerializer(TestJob.class);

      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();
      dao.setConnectionFactory(connectionFactory);
      dao.setNamespace("namespace");
      dao.setExecutions(executions);
      dao.afterPropertiesSet();
   }

   @Test
   void enqueue_normal() {
      TestJob job1 = new TestJob();
      long id1 = dao.enqueue(QUEUE, job1, false).getId();
      assertTrue(id1 > 0);
      TestJob job2 = new TestJob();
      long id2 = dao.enqueue(QUEUE, job2, false).getId();
      assertTrue(id2 > 0);

      Execution execution1 = dao.pop(QUEUE, "worker");
      assertNotNull(execution1);
      assertEquals(id1, execution1.getId());
      assertEquals(job1, execution1.getJob());

      Execution execution2 = dao.pop(QUEUE, "worker");
      assertNotNull(execution2);
      assertEquals(id2, execution2.getId());
      assertEquals(job2, execution2.getJob());
   }

   @Test
   void enqueue_priority() {
      TestJob jobNormal = new TestJob();
      long idNormal = dao.enqueue(QUEUE, jobNormal, false).getId();
      assertTrue(idNormal > 0);
      TestJob jobPriority = new TestJob();
      long idPriority = dao.enqueue(QUEUE, jobPriority, true).getId();
      assertTrue(idPriority > 0);

      // Check that due to the priority flag set to true, the priority job gets executed first, even if scheduled later.
      Execution execution1 = dao.pop(QUEUE, "worker");
      assertNotNull(execution1);
      assertEquals(idPriority, execution1.getId());
      assertEquals(jobPriority, execution1.getJob());

      Execution execution2 = dao.pop(QUEUE, "worker");
      assertNotNull(execution2);
      assertEquals(idNormal, execution2.getId());
      assertEquals(jobNormal, execution2.getJob());
   }

   @Test
   void dequeue() {
      // Nothing to delete -> return false.
      assertFalse(dao.dequeue(QUEUE, Long.MAX_VALUE));

      long id = dao.enqueue(QUEUE, new TestJob(), false).getId();

      boolean dequeued = dao.dequeue(QUEUE, id);

      // Check that the job got dequeued.
      assertTrue(dequeued);
      assertNull(dao.pop(QUEUE, "worker"));
   }

   @Test
   void get() {
      // No job -> return null.
      assertNull(dao.get(Long.MAX_VALUE));

      TestJob job = new TestJob();

      // Add a job.
      dao.enqueue(QUEUE, new TestJob(), false);
      // Add the job, we want to peek.
      long id = dao.enqueue(QUEUE, job, false).getId();
      // Add a job.
      dao.enqueue(QUEUE, new TestJob(), false);

      assertEquals(job, dao.get(id).getJob());
   }

   @Test
   void getQueued() {
      // No job -> return null.
      assertNull(dao.get(Long.MAX_VALUE));


      TestJob job1 = new TestJob();
      dao.enqueue(QUEUE, job1, false);
      TestJob job2 = new TestJob();
      // No in the result, because it already has been "processed".
      long id2 = dao.enqueue(QUEUE, job2, false).getId();
      dao.dequeue(QUEUE, id2);
      // No in the result, because it is in a different queue.
      TestJob job3 = new TestJob();
      dao.enqueue(QUEUE + "2", job3, false);

      assertThat(dao.getQueued(QUEUE).stream().map(Execution::getJob))
            .containsOnly(job1);
   }

   @Test
   void getAll() {
      // No job -> return null.
      assertNull(dao.get(Long.MAX_VALUE));


      TestJob job1 = new TestJob();
      dao.enqueue(QUEUE, job1, false);
      TestJob job2 = new TestJob();
      dao.enqueue(QUEUE, job2, false);
      TestJob job3 = new TestJob();
      dao.enqueue(QUEUE, job3, false);

      assertThat(dao.getAll().stream().map(Execution::getJob))
            .containsOnly(job1, job2, job3);
   }

   @Test
   void cleanUp() {
      TestJob job = new TestJob();
      dao.enqueue(QUEUE, job, false);
      TestJob notDeserializableJob = TestJob.FAILURE;
      long id = dao.enqueue(QUEUE, notDeserializableJob, false).getId();

      assertEquals(1, dao.cleanUp());
      assertNull(dao.get(id));

      // Test, that there is nothing to cleanup now.
      assertEquals(0, dao.cleanUp());
   }
}
