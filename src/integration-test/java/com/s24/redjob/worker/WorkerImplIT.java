package com.s24.redjob.worker;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.QueueDaoImpl;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import com.s24.redjob.worker.events.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.concurrent.TimeUnit;

import static com.s24.redjob.queue.PayloadTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        assertTrue(eventBus.getEvents().isEmpty());
        workerThread.start();

        Object workerStart = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerStart.class, workerStart.getClass());
        assertEquals(worker, ((WorkerStart) workerStart).getWorker());

        Object workerPoll = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerPoll.class, workerPoll.getClass());
        assertEquals(worker, ((WorkerPoll) workerPoll).getWorker());
        assertEquals("test-queue", ((WorkerPoll) workerPoll).getQueue());

        queueDao.enqueue("test-queue", job, false);

        workerPoll = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerPoll.class, workerPoll.getClass());
        assertEquals(worker, ((WorkerPoll) workerPoll).getWorker());
        assertEquals("test-queue", ((WorkerPoll) workerPoll).getQueue());

        Object jobProcess = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobProcess.class, jobProcess.getClass());
        assertEquals(worker, ((JobProcess) jobProcess).getWorker());
        assertEquals("test-queue", ((JobProcess) jobProcess).getQueue());
        assertEquals(job, ((JobProcess) jobProcess).getJob());

        Object jobExecute = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobExecute.class, jobExecute.getClass());
        assertEquals(worker, ((JobExecute) jobExecute).getWorker());
        assertEquals("test-queue", ((JobExecute) jobExecute).getQueue());
        assertEquals(job, ((JobExecute) jobExecute).getJob());

        worker.stop();

        Object jobSuccess = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobSuccess.class, jobSuccess.getClass());
        assertEquals(worker, ((JobSuccess) jobSuccess).getWorker());
        assertEquals("test-queue", ((JobSuccess) jobSuccess).getQueue());
        assertEquals(job, ((JobSuccess) jobSuccess).getJob());

        assertEquals(job, TestJobRunner.getJob());

        Object workerStopped = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerStopped.class, workerStopped.getClass());
        assertEquals(worker, ((WorkerStopped) workerStopped).getWorker());
    }

    @Test
    public void testJobError() throws Exception {
        TestJob job = new TestJob(TestJobRunner.EXCEPTION);

        assertTrue(eventBus.getEvents().isEmpty());
        workerThread.start();

        Object workerStart = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerStart.class, workerStart.getClass());
        assertEquals(worker, ((WorkerStart) workerStart).getWorker());

        Object workerPoll = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerPoll.class, workerPoll.getClass());
        assertEquals(worker, ((WorkerPoll) workerPoll).getWorker());
        assertEquals("test-queue", ((WorkerPoll) workerPoll).getQueue());

        queueDao.enqueue("test-queue", job, false);

        workerPoll = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerPoll.class, workerPoll.getClass());
        assertEquals(worker, ((WorkerPoll) workerPoll).getWorker());
        assertEquals("test-queue", ((WorkerPoll) workerPoll).getQueue());

        Object jobProcess = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobProcess.class, jobProcess.getClass());
        assertEquals(worker, ((JobProcess) jobProcess).getWorker());
        assertEquals("test-queue", ((JobProcess) jobProcess).getQueue());
        assertEquals(job, ((JobProcess) jobProcess).getJob());

        Object jobExecute = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobExecute.class, jobExecute.getClass());
        assertEquals(worker, ((JobExecute) jobExecute).getWorker());
        assertEquals("test-queue", ((JobExecute) jobExecute).getQueue());
        assertEquals(job, ((JobExecute) jobExecute).getJob());

        worker.stop();

        Object jobFailed = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(JobFailed.class, jobFailed.getClass());
        assertEquals(worker, ((JobFailed) jobFailed).getWorker());
        assertEquals("test-queue", ((JobFailed) jobFailed).getQueue());
        assertEquals(job, ((JobFailed) jobFailed).getJob());

        assertEquals(job, TestJobRunner.getJob());

        Object workerStopped = eventBus.waitForEvent(1, TimeUnit.SECONDS);
        assertEquals(WorkerStopped.class, workerStopped.getClass());
        assertEquals(worker, ((WorkerStopped) workerStopped).getWorker());
    }
}
