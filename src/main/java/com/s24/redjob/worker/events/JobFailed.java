package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Worker failed to execute a job.
 */
public class JobFailed {
    /**
     * Worker.
     */
    private final Worker worker;

    /**
     * Queue.
     */
    private final String queue;

    /**
     * Job.
     */
    private final Object job;

    /**
     * Job runner.
     */
    private final Runnable runner;

    /**
     * Constructor.
     *
     * @param worker Worker.
     * @param queue Queue.
     * @param job Job.
     */
    public JobFailed(Worker worker, String queue, Object job, Runnable runner) {
        this.worker = checkNotNull(worker, "Precondition violated: worker != null.");
        this.queue = checkNotNull(queue, "Precondition violated: queue != null.");
        this.job = checkNotNull(job, "Precondition violated: job != null.");
        this.runner = checkNotNull(runner, "Precondition violated: runner != null.");
    }

    /**
     * Worker.
     */
    public Worker getWorker() {
        return worker;
    }

    /**
     * Queue.
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Job.
     */
    public Object getJob() {
        return job;
    }

    /**
     * Job runner.
     */
    public Runnable getRunner() {
        return runner;
    }
}
