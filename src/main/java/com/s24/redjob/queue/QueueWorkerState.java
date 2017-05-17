package com.s24.redjob.queue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.s24.redjob.worker.WorkerState;

/**
 * Queue worker state.
 */
@JsonTypeName
public class QueueWorkerState extends WorkerState {
   /**
    * All concrete queues the worker polls.
    */
   private final Set<String> queues = new HashSet<>();

   /**
    * Constructor.
    */
   public QueueWorkerState() {
   }

   /**
    * All concrete queues the worker polls.
    */
   public Set<String> getQueues() {
      return queues;
   }

   /**
    * All concrete queues the worker polls.
    */
   public void setQueues(Collection<String> queues) {
      this.queues.clear();
      this.queues.addAll(queues);
   }
}
