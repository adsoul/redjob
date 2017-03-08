package com.s24.redjob.channel.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.s24.redjob.queue.QueueWorker;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * Command to shutdown queue workers.
 */
@JsonTypeName
public class PauseQueueWorker {
   /**
    * Pause (true) or unpause (false) workers?.
    */
   private boolean pause;

   /**
    * Workers of which queues should be paused.
    * If empty, all workers will be paused.
    */
   private Set<String> queues = new HashSet<>();

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   PauseQueueWorker() {
   }

   /**
    * Constructor to pause/unpause all workers.
    */
   public PauseQueueWorker(boolean pause) {
      this(pause, emptyList());
   }

   /**
    * Constructor to pause/unpause workers processing the given queues.
    *
    * @param pause
    *           Pause (true) or unpause (false)?
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public PauseQueueWorker(boolean pause, String... queues) {
      this(pause, Arrays.asList(queues));
   }

   /**
    * Constructor to pause/unpause workers processing the given queues.
    *
    * @param pause
    *           Pause (true) or unpause (false)?
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public PauseQueueWorker(boolean pause, Collection<String> queues) {
      this.pause = pause;
      this.queues.addAll(queues);
   }

   /**
    * Pause (true) or unpause (false) workers?.
    */
   public boolean isPause() {
      return pause;
   }

   /**
    * Workers of which queues should be paused.
    * If empty, all workers will be paused.
    */
   public Set<String> getQueues() {
      return queues;
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   protected boolean matches(QueueWorker worker) {
      return queues.isEmpty() || worker.getQueues().stream().anyMatch(queues::contains);
   }
}
