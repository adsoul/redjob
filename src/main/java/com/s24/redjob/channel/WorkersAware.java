package com.s24.redjob.channel;

import java.util.List;

import com.s24.redjob.queue.QueueWorker;

/**
 * Interface for command runners that work on {@link QueueWorker}s.
 */
public interface WorkersAware {
   /**
    * Inject workers.
    */
   void setWorkers(List<QueueWorker> workers);
}
