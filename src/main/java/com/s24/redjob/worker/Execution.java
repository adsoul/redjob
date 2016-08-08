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
   @JsonProperty(value = "created", required = true)
   private Instant created;

   /**
    * Start of execution.
    */
   @JsonInclude(value = Include.NON_NULL)
   @JsonProperty(value = "start", required = false)
   private Instant start;

   /**
    * End of execution.
    */
   @JsonInclude(value = Include.NON_NULL)
   @JsonProperty(value = "end", required = false)
   private Instant end;

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
   public Execution(long id, Object job, Object result) {
      this(id, job, result, Instant.now(), null, null);
   }

   /**
    * Jackson constructor.
    */
   Execution(
         @JsonProperty(value = "id", required = true) long id,
         @JsonProperty(value = "job", required = true) Object job,
         @JsonProperty(value = "result", required = true) Object result,
         @JsonProperty(value = "created", required = true) Instant created,
         @JsonProperty(value = "start", required = false) Instant start,
         @JsonProperty(value = "end", required = false) Instant end) {
      Assert.notNull(job, "Precondition violated: job != null.");
      Assert.notNull(result, "Precondition violated: result != null.");
      // Assert.notNull(created, "Precondition violated: created != null.");

      this.id = id;
      this.job = job;
      this.result = result;
      this.created = Instant.now();
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
   public Instant getCreated() {
      return created;
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
    * End execution.
    */
   public void stop() {
      end = Instant.now();
   }

   /**
    * End of execution.
    */
   public Instant getEnd() {
      return end;
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
