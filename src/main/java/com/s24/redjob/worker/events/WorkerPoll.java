package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Worker polls one of its queues.
 */
public class WorkerPoll {
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
        this.worker = checkNotNull(worker, "Precondition violated: worker != null.");
        this.queue = checkNotNull(queue, "Precondition violated: queue != null.");
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
