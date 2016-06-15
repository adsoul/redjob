package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

/**
 * Worker finished execution of a job.
 */
public interface JobFinished {
   /**
    * Worker.
    */
   <W extends Worker> W getWorker();

   /**
    * Queue.
    */
   String getQueue();

   /**
    * Job.
    */
   <J> J getJob();

   /**
    * Job runner.
    */
   <R> R getRunner();
}
