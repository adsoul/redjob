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
   public void enqueue() {
      String queue = "test";

      long id = dao.enqueue(queue, new TestJob("value"), false).getId();
      assertTrue(id > 0);

      Execution execution = dao.pop(queue, "worker");
      assertNotNull(execution);
      assertEquals(id, execution.getId());
      TestJob job = (TestJob) execution.getJob();
      assertNotNull(job);
      assertEquals("value", job.getValue());
   }
}
