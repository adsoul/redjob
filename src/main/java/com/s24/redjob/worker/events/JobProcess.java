package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Worker processes a job.
 */
public class JobProcess {
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
     * Veto against job execution?.
     */
    private boolean veto = false;

    /**
     * Constructor.
     *
     * @param worker Worker.
     * @param queue Queue.
     * @param job Job.
     */
    public JobProcess(Worker worker, String queue, Object job) {
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

    /**
     * Veto against execution of the job.
     */
    public void veto() {
        this.veto = true;
    }

    /**
     * Has been vetoed against execution of the job?.
     */
    public boolean isVeto() {
        return veto;
    }
}
