package com.s24.redjob.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobFailed;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerNext;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import com.s24.redjob.worker.json.ExecutionRedisSerializer;
import com.s24.redjob.worker.json.TestExecutionRedisSerializer;

/**
 * Integration test for {@link FifoWorker}.
 */
public class FifoWorkerImplIT {
   /**
    * Recording event publisher.
    */
   private TestEventPublisher eventBus = new TestEventPublisher();

   /**
    * Queue DAO.
    */
   private FifoDao fifoDao;

   /**
    * Worker under test.
    */
   private FifoWorker worker;

   /**
    * Worker thread.
    */
   private Thread workerThread;

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory redis = TestRedis.connectionFactory();

      ExecutionRedisSerializer executions = new TestExecutionRedisSerializer(TestJob.class);

      FifoWorkerFactoryBean factory = new FifoWorkerFactoryBean() {
         @Override
         protected void start() {
            // Do not start worker.
         }
      };
      factory.setConnectionFactory(redis);
      factory.setNamespace("namespace");
      factory.setExecutions(executions);
      factory.setQueues("test-queue");
      factory.setJobRunnerFactory(new TestJobRunnerFactory());
      factory.setApplicationEventPublisher(eventBus);
      factory.afterPropertiesSet();

      worker = factory.getObject();
      fifoDao = worker.getFifoDao();
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
      workerThread = worker.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());

      Execution execution = fifoDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", execution, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new JobSuccess(worker, "test-queue", execution, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }

   @Test
   public void testJobError() throws Exception {
      TestJob job = new TestJob(TestJobRunner.EXCEPTION_VALUE);
      TestJobRunner runner = new TestJobRunner(job);

      assertTrue(eventBus.getEvents().isEmpty());
      workerThread = worker.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());

      Execution execution = fifoDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", execution, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new JobFailed(worker, "test-queue", execution, runner, TestJobRunner.EXCEPTION), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }
}
