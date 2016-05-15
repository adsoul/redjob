package com.s24.redjob.worker;

import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.QueueDaoImpl;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.concurrent.TimeUnit;

import static com.s24.redjob.queue.PayloadTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for {@link WorkerImpl}.
 */
@RunWith(MockitoJUnitRunner.class)
public class WorkerImplIT {
    /**
     * Queue DAO.
     */
    private QueueDaoImpl queueDao = new QueueDaoImpl();

    /**
     * Worker DAO.
     */
    private WorkerDaoImpl workerDao = new WorkerDaoImpl();

    @Mock
    private ApplicationEventPublisher applicationEventPublisher;

    /**
     * Worker under test.
     */
    @InjectMocks
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
    public void poll() throws Exception {
        TestJobRunner.resetLatch(1);
        TestJob job = new TestJob("worker");
        queueDao.enqueue("test-queue", job, false);

        workerThread.start();

        assertTrue(TestJobRunner.awaitLatch(10, TimeUnit.SECONDS));
        assertEquals(job, TestJobRunner.getJob());
    }
}
