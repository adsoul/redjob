package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * New worker starts.
 */
public class WorkerStart {
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
        this.worker = checkNotNull(worker, "Precondition violated: worker != null.");
    }

    /**
     * Worker.
     */
    public Worker getWorker() {
        return worker;
    }
}
