package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Worker polls one of its queues.
 */
public class WorkerPoll extends ApplicationEvent implements WorkerEvent, QueueEvent {
   /**
    * Worker.
    */
   private final Worker worker;

   /**
    * Queue.
    */
   private final String queue;

   /**
    * Veto against job execution?.
    */
   private boolean veto = false;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker.
    * @param queue
    *           Queue.
    */
   public WorkerPoll(Worker worker, String queue) {
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

   /**
    * Veto against execution of the job.
    */
   public void veto() {
      this.veto = true;
   }

   /**
    * Has been vetoed against execution of the job?.
    */
   public boolean isVeto() {
      return veto;
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
