package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker stopped.
 */
public class WorkerStopped extends ApplicationEvent {
    /**
     * Worker.
     */
    private final Worker worker;

    /**
     * Constructor.
     *
     * @param worker Worker.
     */
    public WorkerStopped(Worker worker) {
        super(worker);
        Assert.notNull(worker, "Precondition violated: worker != null.");
        this.worker = worker;
    }

    /**
     * Worker.
     */
    public Worker getWorker() {
        return worker;
    }
}
