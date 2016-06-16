package com.s24.redjob.worker.events;

import com.s24.redjob.worker.JobRunner;

/**
 * Worker finished execution of a job.
 */
public interface JobFinished extends JobEvent {
   /**
    * Job runner.
    */
   <J, R extends JobRunner<J>> R getRunner();
}
