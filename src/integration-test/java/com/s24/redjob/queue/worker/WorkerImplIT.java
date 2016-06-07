package com.s24.redjob.queue.worker;

import static com.s24.redjob.queue.JobTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.QueueDaoImpl;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import com.s24.redjob.queue.worker.events.JobExecute;
import com.s24.redjob.queue.worker.events.JobFailed;
import com.s24.redjob.queue.worker.events.JobProcess;
import com.s24.redjob.queue.worker.events.JobSuccess;
import com.s24.redjob.queue.worker.events.WorkerPoll;
import com.s24.redjob.queue.worker.events.WorkerStart;
import com.s24.redjob.queue.worker.events.WorkerStopped;

/**
 * Integration test for {@link WorkerImpl}.
 */
public class WorkerImplIT {
   /**
    * Queue DAO.
    */
   private QueueDaoImpl queueDao = new QueueDaoImpl();

   /**
    * Worker DAO.
    */
   private WorkerDaoImpl workerDao = new WorkerDaoImpl();

   /**
    * Recording event publisher.
    */
   private TestEventPublisher eventBus = new TestEventPublisher();

   /**
    * Worker under test.
    */
   private WorkerImpl worker = new WorkerImpl();

   /**
    * Worker thread.
    */
   private Thread workerThread;

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory redis = TestRedis.connectionFactory();

      queueDao.setConnectionFactory(redis);
      queueDao.setNamespace("namespace");
      queueDao.afterPropertiesSet();
      scanForJsonSubtypes(queueDao.getJson(), TestJob.class);

      workerDao.setConnectionFactory(redis);
      workerDao.setNamespace("namespace");
      workerDao.afterPropertiesSet();

      worker.setQueues("test-queue");
      worker.setQueueDao(queueDao);
      worker.setWorkerDao(workerDao);
      worker.setJobRunnerFactory(new TestJobRunnerFactory());
      worker.setApplicationEventPublisher(eventBus);
      worker.afterPropertiesSet();

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
      TestJob job = new TestJob("worker");
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
