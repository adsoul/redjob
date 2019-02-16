package com.s24.redjob.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to clean up jobs. This job deletes all not deserializable job executions.
 */
@JsonTypeName
public class CleanUpJobs {
   /**
    * Constructor to clean up the current namespace.
    */
   @JsonCreator
   public CleanUpJobs() { }
}
