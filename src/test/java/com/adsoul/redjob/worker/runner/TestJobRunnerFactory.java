package com.adsoul.redjob.worker.runner;

import com.adsoul.redjob.worker.Execution;

/**
 * {@link JobRunnerFactory} for {@link TestJob}s.
 */
public class TestJobRunnerFactory implements JobRunnerFactory {
   @Override
   public Runnable runnerFor(Execution execution) {
      TestJob job = execution.getJob();
      return new TestJobRunner(job);
   }
}
