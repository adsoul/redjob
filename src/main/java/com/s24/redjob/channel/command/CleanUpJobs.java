package com.s24.redjob.channel.command;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.s24.redjob.queue.QueueWorker;

/**
 * Command to clean up jobs. This job deletes all not deserializable job executions.
 */
@JsonTypeName
public class CleanUpJobs {
   /**
    * Workers of which the queues should be cleaned up.
    * If empty, all workers will be cleaned up.
    */
   private Set<String> queues = new HashSet<>();

   /**
    * Constructor to clean up workers processing the given queues.
    *
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public CleanUpJobs(String... queues) {
      this(Arrays.asList(queues));
   }

   /**
    * Constructor to clean up workers processing the given queues.
    *
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public CleanUpJobs(Collection<String> queues) {
      this.queues.addAll(queues);
   }

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   CleanUpJobs() {
   }

   /**
    * Workers of which the queues should be cleaned up.
    * If empty, all workers will be cleaned up.
    */
   public Set<String> getQueues() {
      return queues;
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   protected boolean matches(QueueWorker worker) {
      return queues.isEmpty() || worker.getQueues().stream().filter(queues::contains).findAny().isPresent();
   }
}
