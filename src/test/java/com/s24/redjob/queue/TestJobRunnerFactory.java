package com.s24.redjob.queue;

import org.springframework.util.Assert;

import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerFactory;

/**
 * {@link JobRunnerFactory} for {@link TestJob}s.
 */
public class TestJobRunnerFactory implements JobRunnerFactory {
   @Override
   public <J> JobRunner<J> runnerFor(J job) {
      Assert.isInstanceOf(TestJob.class, job, "Precondition violated: ");
      return (JobRunner<J>) new TestJobRunner();
   }
}
