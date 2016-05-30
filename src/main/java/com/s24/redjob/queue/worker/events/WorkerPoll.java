package com.s24.redjob.queue.worker.events;

import com.s24.redjob.queue.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        return o instanceof WorkerPoll &&
                Objects.equals(worker, ((WorkerPoll) o).worker) &&
                Objects.equals(queue, ((WorkerPoll) o).queue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(worker, queue);
    }
}
