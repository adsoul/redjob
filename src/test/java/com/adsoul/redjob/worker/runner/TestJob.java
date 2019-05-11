package com.adsoul.redjob.worker.runner;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Test job.
 */
@JsonTypeName("testJob")
public class TestJob {
   /**
    * Failing job.
    */
   public static final TestJob FAILURE = new TestJob();
   static {
      FAILURE.value = "fail";
   }

   /**
    * A value.
    */
   @JsonProperty("value")
   private String value;

   /**
    * Constructor using a random value.
    */
   public TestJob() {
      this(UUID.randomUUID().toString());
   }

   /**
    * Constructor.
    *
    * @param value
    *           A value.
    */
   @JsonCreator
   public TestJob(@JsonProperty("value") String value) {
      this.value = value;
      // Throw exception on deserialization, if requested.
      if ("fail".equals(value)) {
         throw new IllegalArgumentException("Failure");
      }
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
