package com.s24.resque.queue;

import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.s24.resque.TestConnection;
import org.junit.Before;
import org.junit.Test;

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
        dao.setConnectionFactory(TestConnection.connectionFactory());
        dao.setNamespace("namespace");

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

    /**
     * Test payload.
     */
    @JsonTypeName("testPayload")
    public static class TestPayload {
        /**
         * A value.
         */
        private String value;

        /**
         * Constructor.
         *
         * @param value A value.
         */
        public TestPayload(String value) {
            this.value = value;
        }

        /**
         * A value.
         */
        public String getValue() {
            return value;
        }
    }
}
