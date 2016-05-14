package com.s24.redjob.queue;

import org.springframework.util.Assert;

import com.s24.redjob.worker.JobRunnerFactory;

/**
 * {@link JobRunnerFactory} for {@link TestJob}s.
 */
public class TestJobRunnerFactory implements JobRunnerFactory {
    @Override
    public Runnable runnerFor(Object job) {
        Assert.isInstanceOf(TestJob.class, job, "Precondition violated: ");
        return new TestJobRunner((TestJob) job);
    }
}
