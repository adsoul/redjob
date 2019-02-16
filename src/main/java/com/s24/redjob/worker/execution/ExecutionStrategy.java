package com.s24.redjob.worker.execution;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.runner.JobRunner;

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
