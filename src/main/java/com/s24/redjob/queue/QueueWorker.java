package com.s24.redjob.queue;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

/**
 * {@link Worker} handling queues.
 */
public interface QueueWorker extends Worker {
   /**
    * Pause / unpause worker.
    */
   void pause(boolean pause);

   /**
    * Stop the given job.
    *
    * @param id
    *           Job id.
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
