package com.s24.redjob.queue;

import static com.s24.redjob.queue.TypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.worker.ExecutionRedisSerializer;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobFailed;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * Integration test for {@link QueueWorker}.
 */
public class QueueWorkerImplIT {
   /**
    * Recording event publisher.
    */
   private TestEventPublisher eventBus = new TestEventPublisher();

   /**
    * Queue DAO.
    */
   private QueueDao queueDao;

   /**
    * Worker under test.
    */
   private QueueWorker worker;

   /**
    * Worker thread.
    */
   private Thread workerThread;

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory redis = TestRedis.connectionFactory();

      ExecutionRedisSerializer executions = new ExecutionRedisSerializer();
      scanForJsonSubtypes(executions, TestJob.class);

      QueueWorkerFactoryBean factory = new QueueWorkerFactoryBean();
      factory.setConnectionFactory(redis);
      factory.setNamespace("namespace");
      factory.setExecutions(executions);
      factory.setQueues("test-queue");
      factory.setJobRunnerFactory(new TestJobRunnerFactory());
      factory.setApplicationEventPublisher(eventBus);
      factory.afterPropertiesSet();

      worker = factory.getObject();
      queueDao = worker.getQueueDao();

      workerThread = new Thread(worker, "test-worker");
   }

   @After
   public void tearDown() throws Exception {
      worker.stop();
      workerThread.join(1000);
      workerThread.stop();
   }

   @Test
   public void testLifecycle() throws Exception {
      TestJob job = new TestJob();
      TestJobRunner runner = new TestJobRunner(job);

      assertTrue(eventBus.getEvents().isEmpty());
      workerThread.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());

      queueDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", job), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", job, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new JobSuccess(worker, "test-queue", job, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getJob());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }

   @Test
   public void testJobError() throws Exception {
      TestJob job = new TestJob(TestJobRunner.EXCEPTION);
      TestJobRunner runner = new TestJobRunner(job);

      assertTrue(eventBus.getEvents().isEmpty());
      workerThread.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());

      queueDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", job), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", job, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new JobFailed(worker, "test-queue", job, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getJob());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }
}
