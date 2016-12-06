package com.s24.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.s24.redjob.worker.Worker;

/**
 * Worker failed.
 */
public class WorkerFailed extends ApplicationEvent implements WorkerEvent {
   /**
    * Worker.
    */
   private final Worker worker;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker.
    */
   public WorkerFailed(Worker worker) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      this.worker = worker;
   }

   @Override
   public <W extends Worker> W getWorker() {
      return (W) worker;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof WorkerFailed &&
            Objects.equals(worker, ((WorkerFailed) o).worker);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker);
   }
}
