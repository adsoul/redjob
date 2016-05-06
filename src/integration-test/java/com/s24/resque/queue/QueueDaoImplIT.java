package com.s24.resque.queue;

import static com.s24.resque.queue.PayloadTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.s24.resque.TestRedis;

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

        scanForJsonSubtypes(dao.getJson(), getClass());
    }

    @Test
    public void enqueue() {
        String queue = "test";

        long id = dao.enqueue(queue, new TestPayload("value"), false);
        assertTrue(id > 0);

        Job job = dao.pop(queue);
        assertNotNull(job);
        assertEquals(id, job.getId());
        TestPayload payload = (TestPayload) job.getPayload();
        assertNotNull(payload);
        assertEquals("value", payload.getValue());
    }

}
