package com.adsoul.redjob.queue;

import java.util.List;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.Worker;

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
