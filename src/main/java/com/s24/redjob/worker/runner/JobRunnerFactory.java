package com.s24.redjob.worker.runner;

import com.s24.redjob.worker.Execution;

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
