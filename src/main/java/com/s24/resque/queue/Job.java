package com.s24.resque.queue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.springframework.util.Assert;

/**
 * Job stored as JSON into queue.
 */
public class Job {
    /**
     * Id of job.
     */
    @JsonProperty(value = "id", required = true)
    private final long id;

    /**
     * Payload.
     */
    @JsonProperty(value = "payload", required = true)
    @JsonTypeInfo(use = Id.NAME, include = As.EXTERNAL_PROPERTY, property = "payloadType")
    private final Object payload;

    /**
     * Constructor.
     *
     * @param id Id of job.
     * @param payload Payload.
     */
    public Job(
            @JsonProperty(value = "id", required = true) long id,
            @JsonProperty(value = "payload", required = true) Object payload) {
        Assert.notNull(payload, "Precondition violated: payload != null.");

        this.id = id;
        this.payload = payload;
    }

    /**
     * Id of the job.
     */
    public long getId() {
        return id;
    }

    /**
     * Payload.
     */
    public Object getPayload() {
        return payload;
    }
}
