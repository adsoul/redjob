package com.adsoul.redjob.worker.runner;

import com.adsoul.redjob.worker.Execution;

/**
 * Interface for job runners.
 *
 * @param <J>
 *       Job.
 */
public interface JobRunner<J> {
   /**
    * Run job.
    */
   void run(Execution execution);
}
