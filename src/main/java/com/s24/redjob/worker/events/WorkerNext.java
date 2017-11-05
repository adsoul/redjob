package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Worker polled one of its queues and will be advancing to the next.
 */
public class WorkerNext extends ApplicationEvent implements WorkerEvent, QueueEvent {
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
    * @param worker
    *           Worker.
    * @param queue
    *           Queue.
    */
   public WorkerNext(Worker worker, String queue) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      this.worker = worker;
      this.queue = queue;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <W extends Worker> W getWorker() {
      return (W) worker;
   }

   /**
    * Queue.
    */
   @Override
   public String getQueue() {
      return queue;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof WorkerNext &&
            Objects.equals(worker, ((WorkerNext) o).worker) &&
            Objects.equals(queue, ((WorkerNext) o).queue);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue);
   }
}
