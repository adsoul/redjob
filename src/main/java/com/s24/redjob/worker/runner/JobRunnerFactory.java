package com.s24.redjob.worker.runner;

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
   <J> Runnable runnerFor(J job);
}
