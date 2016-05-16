package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Worker got an exception.
 */
public class WorkerError extends ApplicationEvent {
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
        super(worker);
        Assert.notNull(worker, "Precondition violated: worker != null.");
        Assert.notNull(throwable, "Precondition violated: throwable != null.");
        this.worker = worker;
        this.throwable = throwable;
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

    @Override
    public boolean equals(Object o) {
        return o instanceof WorkerError &&
                Objects.equals(worker, ((WorkerError) o).worker) &&
                Objects.equals(throwable, ((WorkerError) o).throwable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(worker, throwable);
    }
}
