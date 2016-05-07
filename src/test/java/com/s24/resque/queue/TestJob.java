package com.s24.resque.queue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Test job.
 */
@JsonTypeName("testJob")
public class TestJob {
    /**
     * A value.
     */
    @JsonProperty("value")
    private String value;

    /**
     * Constructor.
     *
     * @param value A value.
     */
    public TestJob(@JsonProperty("value") String value) {
        this.value = value;
    }

    /**
     * A value.
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TestJob && value.equals(((TestJob) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
