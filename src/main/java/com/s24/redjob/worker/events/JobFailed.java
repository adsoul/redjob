package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker failed to execute a job.
 */
public class JobFailed extends ApplicationEvent implements JobFinished {
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
     * @param runner Job runner.
     */
    public JobFailed(Worker worker, String queue, Object job, Runnable runner) {
        super(worker);
        Assert.notNull(worker, "Precondition violated: worker != null.");
        Assert.hasLength(queue, "Precondition violated: queue has length.");
        Assert.notNull(job, "Precondition violated: job != null.");
        Assert.notNull(runner, "Precondition violated: runner != null.");
        this.worker = worker;
        this.queue = queue;
        this.job = job;
        this.runner = runner;
    }

    @Override
    public Worker getWorker() {
        return worker;
    }

    @Override
    public String getQueue() {
        return queue;
    }

    @Override
    public Object getJob() {
        return job;
    }

    @Override
    public Runnable getRunner() {
        return runner;
    }
}
