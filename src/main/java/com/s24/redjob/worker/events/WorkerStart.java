package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * New worker starts.
 */
public class WorkerStart extends ApplicationEvent {
    /**
     * Worker.
     */
    private final Worker worker;

    /**
     * Constructor.
     *
     * @param worker Worker.
     */
    public WorkerStart(Worker worker) {
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
