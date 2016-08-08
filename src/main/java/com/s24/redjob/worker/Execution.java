package com.s24.redjob.worker;

import java.time.Instant;

import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
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
    * Job result.
    */
   @JsonProperty(value = "result", required = true)
   @JsonTypeInfo(use = Id.NAME, include = As.EXTERNAL_PROPERTY, property = "resultType")
   private Object result;

   /**
    * Creation of execution.
    */
   @JsonProperty(value = "create", required = true)
   private Instant creation;

   /**
    * Start of execution.
    */
   @JsonInclude(value = Include.NON_NULL)
   @JsonProperty(value = "start", required = false)
   private Instant start;

   /**
    * Stop of execution.
    */
   @JsonInclude(value = Include.NON_NULL)
   @JsonProperty(value = "stop", required = false)
   private Instant stop;

   /**
    * Constructor.
    *
    * @param id
    *           Id of job.
    * @param job
    *           Job.
    */
   public Execution(long id, Object job) {
      this(id, job, new NoResult());
   }

   /**
    * Constructor.
    *
    * @param id
    *           Id of job.
    * @param job
    *           Job.
    * @param result
    *           Job result.
    */
   public Execution(
         @JsonProperty(value = "id", required = true) long id,
         @JsonProperty(value = "job", required = true) Object job,
         @JsonProperty(value = "result", required = true) Object result) {
      Assert.notNull(job, "Precondition violated: job != null.");
      Assert.notNull(job, "Precondition violated: result != null.");

      this.id = id;
      this.job = job;
      this.result = result;
      this.creation = Instant.now();
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
   @SuppressWarnings("unchecked")
   public <J> J getJob() {
      return (J) job;
   }

   /**
    * Job result.
    */
   @SuppressWarnings("unchecked")
   public <R> R getResult() {
      return (R) result;
   }

   /**
    * Job result.
    */
   public void setResult(Object result) {
      this.result = result;
   }

   /**
    * Creation of execution.
    */
   public Instant getCreation() {
      return creation;
   }

   /**
    * Start execution.
    */
   public void start() {
      start = Instant.now();
   }

   /**
    * Start of execution.
    */
   public Instant getStart() {
      return start;
   }

   /**
    * Stop execution.
    */
   public void stop() {
      stop = Instant.now();
   }

   /**
    * Stop of execution.
    */
   public Instant getStop() {
      return stop;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof Execution &&
            id == ((Execution) o).id &&
            job.equals(((Execution) o).job);
   }

   @Override
   public int hashCode() {
      return Long.hashCode(id);
   }
}
