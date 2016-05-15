package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Worker has skipped a job due to a veto.
 */
public class JobSkipped {
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
     * Constructor.
     *
     * @param worker Worker.
     * @param queue Queue.
     * @param job Job.
     */
    public JobSkipped(Worker worker, String queue, Object job) {
        this.worker = checkNotNull(worker, "Precondition violated: worker != null.");
        this.queue = checkNotNull(queue, "Precondition violated: queue != null.");
        this.job = checkNotNull(job, "Precondition violated: job != null.");
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
}
