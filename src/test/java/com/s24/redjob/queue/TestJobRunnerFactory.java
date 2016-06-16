package com.s24.redjob.queue;

import com.s24.redjob.worker.JobRunnerFactory;
import org.springframework.util.Assert;

/**
 * {@link JobRunnerFactory} for {@link TestJob}s.
 */
public class TestJobRunnerFactory implements JobRunnerFactory {
   @Override
   public <J> Runnable runnerFor(J job) {
      Assert.isInstanceOf(TestJob.class, job, "Precondition violated: ");
      return new TestJobRunner((TestJob) job);
   }
}
