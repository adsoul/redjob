package com.s24.redjob.queue;

import static com.s24.redjob.queue.JobTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.s24.redjob.TestRedis;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.ExecutionRedisSerializer;

/**
 * Integration test for {@link QueueDaoImpl}.
 */
public class QueueDaoImplIT {
   /**
    * DAO under test.
    */
   private QueueDaoImpl dao = new QueueDaoImpl();

   @Before
   public void setUp() throws Exception {
      ExecutionRedisSerializer executions = new ExecutionRedisSerializer();
      scanForJsonSubtypes(executions, TestJob.class);

      dao.setConnectionFactory(TestRedis.connectionFactory());
      dao.setNamespace("namespace");
      dao.setExecutions(executions);
      dao.afterPropertiesSet();
   }

   @Test
   public void enqueue_normal() {
      String queue = "test";

      TestJob job1 = new TestJob();
      long id1 = dao.enqueue(queue, job1, false).getId();
      assertTrue(id1 > 0);
      TestJob job2 = new TestJob();
      long id2 = dao.enqueue(queue, job2, false).getId();
      assertTrue(id2 > 0);

      Execution execution1 = dao.pop(queue, "worker");
      assertNotNull(execution1);
      assertEquals(id1, execution1.getId());
      assertEquals(job1, execution1.getJob());

      Execution execution2 = dao.pop(queue, "worker");
      assertNotNull(execution2);
      assertEquals(id2, execution2.getId());
      assertEquals(job2, execution2.getJob());
   }

   @Test
   public void enqueue_priority() {
      String queue = "test";

      TestJob jobNormal = new TestJob();
      long idNormal = dao.enqueue(queue, jobNormal, false).getId();
      assertTrue(idNormal > 0);
      TestJob jobPriority = new TestJob();
      long idPriority = dao.enqueue(queue, jobPriority, true).getId();
      assertTrue(idPriority > 0);

      // Check that due to the priority flag set to true, the priority job gets executed first, even if scheduled later.
      Execution execution1 = dao.pop(queue, "worker");
      assertNotNull(execution1);
      assertEquals(idPriority, execution1.getId());
      assertEquals(jobPriority, execution1.getJob());

      Execution execution2 = dao.pop(queue, "worker");
      assertNotNull(execution2);
      assertEquals(idNormal, execution2.getId());
      assertEquals(jobNormal, execution2.getJob());
   }
}
