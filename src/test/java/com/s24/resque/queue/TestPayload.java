package com.s24.resque.queue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Test payload.
 */
@JsonTypeName("testPayload")
public class TestPayload {
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
    public TestPayload(@JsonProperty("value") String value) {
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
        return o instanceof TestPayload && value.equals(((TestPayload) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
