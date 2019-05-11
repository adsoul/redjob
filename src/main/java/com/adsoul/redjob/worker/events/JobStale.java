package com.adsoul.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.Worker;

/**
 * Worker deleted a stale job.
 */
public class JobStale extends ApplicationEvent implements JobEvent {
   /**
    * Worker.
    */
   private final Worker worker;

   /**
    * Queue.
    */
   private final String queue;

   /**
    * Job execution.
    */
   private final Execution execution;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker.
    * @param queue
    *           Queue.
    * @param execution
    *           Job execution.
    */
   public JobStale(Worker worker, String queue, Execution execution) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(execution, "Precondition violated: execution != null.");
      this.worker = worker;
      this.queue = queue;
      this.execution = execution;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <W extends Worker> W getWorker() {
      return (W) worker;
   }

   @Override
   public String getQueue() {
      return queue;
   }

   @Override
   public Execution getExecution() {
      return execution;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof JobStale &&
            Objects.equals(worker, ((JobStale) o).worker) &&
            Objects.equals(queue, ((JobStale) o).queue) &&
            Objects.equals(execution, ((JobStale) o).execution);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
