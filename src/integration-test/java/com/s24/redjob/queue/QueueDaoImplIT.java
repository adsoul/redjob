package com.s24.redjob.queue;

import com.s24.redjob.TestRedis;
import org.junit.Before;
import org.junit.Test;

import static com.s24.redjob.queue.PayloadTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.*;

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
        dao.setConnectionFactory(TestRedis.connectionFactory());
        dao.setNamespace("namespace");
        dao.afterPropertiesSet();

        scanForJsonSubtypes(dao.getJson(), TestJob.class);
    }

    @Test
    public void enqueue() {
        String queue = "test";

        long id = dao.enqueue(queue, new TestJob("value"), false);
        assertTrue(id > 0);

        Job job = dao.pop(queue, "worker");
        assertNotNull(job);
        assertEquals(id, job.getId());
        TestJob payload = (TestJob) job.getPayload();
        assertNotNull(payload);
        assertEquals("value", payload.getValue());
    }
}
