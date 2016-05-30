package com.s24.redjob.queue.worker.events;

import com.s24.redjob.queue.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        return o instanceof WorkerStopped &&
                Objects.equals(worker, ((WorkerStopped) o).worker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(worker);
    }
}
