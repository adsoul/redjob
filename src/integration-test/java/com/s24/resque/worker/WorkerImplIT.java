package com.s24.resque.worker;

import static com.s24.resque.queue.PayloadTypeScannerTest.scanForJsonSubtypes;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.s24.resque.TestRedis;
import com.s24.resque.queue.QueueDaoImpl;
import com.s24.resque.queue.TestPayload;

/**
 * Integration test for {@link WorkerImpl}.
 */
@RunWith(MockitoJUnitRunner.class)
public class WorkerImplIT {
    /**
     * Queue DAO.
     */
    private QueueDaoImpl dao = new QueueDaoImpl();

    /**
     * Job runner factory.
     */
    @Mock
    private JobRunnerFactory jobRunnerFactory;

    /**
     * Job runner.
     */
    @Mock
    private Runnable jobRunner;

    /**
     * Worker under test.
     */
    @InjectMocks
    private WorkerImpl worker;

    /**
     * Worker thread.
     */
    private Thread workerThread;

    @Before
    public void setUp() throws Exception {
        dao.setConnectionFactory(TestRedis.connectionFactory());
        dao.setNamespace("namespace");

        scanForJsonSubtypes(dao.getJson(), TestPayload.class);

        worker.setQueueDao(dao);
        worker.setQueues("test-queue");

        workerThread = new Thread(worker, "test-worker");
    }

    @After
    public void tearDown() throws Exception {
        workerThread.stop();
    }

    @Test
    public void poll() throws Exception {
        TestPayload payload = new TestPayload("worker");
        when(jobRunnerFactory.runnerFor(payload)).thenReturn(jobRunner);

        dao.enqueue("test-queue", payload, false);

        workerThread.start();
        Thread.sleep(3000);

        verify(jobRunnerFactory).runnerFor(payload);
        verify(jobRunner).run();
    }
}
