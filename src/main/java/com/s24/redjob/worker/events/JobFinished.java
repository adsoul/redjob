package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

/**
 * Worker finished execution of a job.
 */
public interface JobFinished {
   /**
    * Worker.
    */
   Worker getWorker();

   /**
    * Queue.
    */
   String getQueue();

   /**
    * Job.
    */
   Object getJob();

   /**
    * Job runner.
    */
   Runnable getRunner();
}
