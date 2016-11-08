package com.s24.redjob.channel.command;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.s24.redjob.queue.QueueWorker;

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
    * Workers of which queues should be paused?.
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
      this.pause = pause;
   }

   /**
    * Pause (true) or unpause (false) workers?.
    */
   public boolean isPause() {
      return pause;
   }

   /**
    * Workers of which queues should be paused?.
    * If empty, all workers will be paused.
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
