package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Worker got an exception.
 */
public class WorkerError {
    /**
     * Worker.
     */
    private final Worker worker;

    /**
     * Throwable.
     */
    private final Throwable throwable;

    /**
     * Constructor.
     *
     * @param worker Worker.
     */
    public WorkerError(Worker worker, Throwable throwable) {
        this.worker = checkNotNull(worker, "Precondition violated: worker != null.");
        this.throwable = checkNotNull(throwable, "Precondition violated: throwable != null.");
    }

    /**
     * Worker.
     */
    public Worker getWorker() {
        return worker;
    }

    /**
     * Throwable.
     */
    public Throwable getThrowable() {
        return throwable;
    }
}
