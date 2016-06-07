package com.s24.redjob.queue;

import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * Job execution. Stored as JSON in Redis.
 */
public class Execution {
   /**
    * Id of job.
    */
   @JsonProperty(value = "id", required = true)
   private final long id;

   /**
    * Job.
    */
   @JsonProperty(value = "job", required = true)
   @JsonTypeInfo(use = Id.NAME, include = As.EXTERNAL_PROPERTY, property = "jobType")
   private final Object job;

   /**
    * Constructor.
    *
    * @param id
    *           Id of job.
    * @param job
    *           Job.
    */
   public Execution(
         @JsonProperty(value = "id", required = true) long id,
         @JsonProperty(value = "job", required = true) Object job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      this.id = id;
      this.job = job;
   }

   /**
    * Id of the job.
    */
   public long getId() {
      return id;
   }

   /**
    * Job.
    */
   public Object getJob() {
      return job;
   }
}
