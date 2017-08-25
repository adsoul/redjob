package com.s24.redjob.queue;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

import java.util.List;

/**
 * {@link Worker} handling queues.
 */
public interface QueueWorker extends Worker {
   /**
    * Queues to listen to.
    */
   List<String> getQueues();

   /**
    * Pause / unpause worker.
    */
   void pause(boolean pause);

   /**
    * Stop the given execution.
    *
    * @param id
    *           Execution id.
    */
   void stop(long id);

   /**
    * Update execution.
    *
    * @param execution
    *           Execution.
    */
   void update(Execution execution);
}
