package com.adsoul.redjob.worker.execution;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.runner.JobRunner;

/**
 * Strategy for workers how to execute {@link JobRunner}s.
 */
public interface ExecutionStrategy {
   /**
    * Execute a {@link JobRunner}.
    *
    *
    * @param queue
    *       Name of queue.
    * @param execution
    *       Job.
    */
   void execute(String queue, Execution execution);
}
