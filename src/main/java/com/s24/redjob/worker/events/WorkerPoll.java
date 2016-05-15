package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker polls one of its queues.
 */
public class WorkerPoll extends ApplicationEvent {
    /**
     * Worker.
     */
    private final Worker worker;

    /**
     * Queue.
     */
    private final String queue;

    /**
     * Constructor.
     *
     * @param worker Worker.
     * @param queue Queue.
     */
    public WorkerPoll(Worker worker, String queue) {
        super(worker);
        Assert.notNull(worker, "Precondition violated: worker != null.");
        Assert.hasLength(queue, "Precondition violated: queue has length.");
        this.worker = worker;
        this.queue = queue;
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
}
