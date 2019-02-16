package com.s24.redjob.worker.runner;

import com.s24.redjob.worker.Execution;

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
