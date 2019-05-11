package com.adsoul.redjob.worker.runner;

import com.adsoul.redjob.worker.Execution;

/**
 * Interface for factories creating job runners.
 */
public interface JobRunnerFactory {
   /**
    * Get runner for job.
    *
    * @param execution
    *           Job execution.
    * @return Job runner.
    */
   Runnable runnerFor(Execution execution);
}
