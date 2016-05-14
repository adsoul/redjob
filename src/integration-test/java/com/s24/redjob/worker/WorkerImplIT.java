package com.s24.redjob.worker;

import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.QueueDaoImpl;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private QueueDaoImpl dao = new QueueDaoImpl();

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
        dao.setConnectionFactory(TestRedis.connectionFactory());
        dao.setNamespace("namespace");
        dao.afterPropertiesSet();

        scanForJsonSubtypes(dao.getJson(), TestJob.class);

        worker.setQueues("test-queue");
        worker.setQueueDao(dao);
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
        dao.enqueue("test-queue", job, false);

        workerThread.start();

        assertTrue(TestJobRunner.awaitLatch(10, TimeUnit.SECONDS));
        assertEquals(job, TestJobRunner.getJob());
    }
}
