package com.s24.redjob.queue;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.WorkerDaoImpl;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobFailure;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobStart;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerNext;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import com.s24.redjob.worker.events.WorkerStopping;
import com.s24.redjob.worker.json.TestExecutionRedisSerializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link FifoWorker}.
 */
class FifoWorkerImplIT {
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

   @BeforeEach
   void setUp() throws Exception {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();

      WorkerDaoImpl workerDao = new WorkerDaoImpl();
      workerDao.setConnectionFactory(connectionFactory);
      workerDao.setNamespace("test");
      workerDao.afterPropertiesSet();

      FifoDaoImpl fifoDao = new FifoDaoImpl();
      fifoDao.setConnectionFactory(connectionFactory);
      fifoDao.setNamespace("test");
      fifoDao.setExecutions(new TestExecutionRedisSerializer(TestJob.class));
      fifoDao.afterPropertiesSet();

      FifoWorkerFactoryBean factory = new FifoWorkerFactoryBean();
      factory.setWorkerDao(workerDao);
      factory.setFifoDao(fifoDao);
      factory.setQueues("test-queue");
      factory.setJobRunnerFactory(new TestJobRunnerFactory());
      factory.setApplicationEventPublisher(eventBus);
      factory.afterPropertiesSet();

      worker = factory.getObject();
      this.fifoDao = worker.getFifoDao();
   }

   @AfterEach
   void tearDown() {
      worker.stop();
   }

   @Test
   void testLifecycle() throws Exception {
      TestJob job = new TestJob();
      TestJobRunner runner = new TestJobRunner(job);

      assertTrue(eventBus.getEvents().isEmpty());
      worker.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());

      Execution execution = fifoDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", execution, runner), eventBus.waitForEvent());
      assertEquals(new JobStart(worker, "test-queue", execution, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new JobSuccess(worker, "test-queue", execution, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }

   @Test
   void testJobError() throws Exception {
      TestJob job = new TestJob(TestJobRunner.EXCEPTION_VALUE);
      TestJobRunner runner = new TestJobRunner(job);

      assertTrue(eventBus.getEvents().isEmpty());
      worker.start();

      assertEquals(new WorkerStart(worker), eventBus.waitForEvent());
      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());

      Execution execution = fifoDao.enqueue("test-queue", job, false);

      assertEquals(new WorkerPoll(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new JobProcess(worker, "test-queue", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(worker, "test-queue", execution, runner), eventBus.waitForEvent());
      assertEquals(new JobStart(worker, "test-queue", execution, runner), eventBus.waitForEvent());

      worker.stop();

      assertEquals(new WorkerStopping(worker), eventBus.waitForEvent());
      assertEquals(new JobFailure(worker, "test-queue", execution, runner, TestJobRunner.EXCEPTION), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());
      assertEquals(new WorkerNext(worker, "test-queue"), eventBus.waitForEvent());
      assertEquals(new WorkerStopped(worker), eventBus.waitForEvent());
   }
}
