package com.s24.redjob.worker;

/**
 * Interface for factories creating job runners.
 */
public interface JobRunnerFactory {
   /**
    * Get runner for job.
    *
    * @param job
    *           Job.
    * @return Job runner.
    */
   Runnable runnerFor(Object job);
}
