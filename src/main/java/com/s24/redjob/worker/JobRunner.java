package com.s24.redjob.worker;

/**
 * Interface for job runners.
 */
public interface JobRunner<J> {
   /**
    * Execute job.
    *
    * @param job
    *           Job.
    */
   void execute(J job);
}
