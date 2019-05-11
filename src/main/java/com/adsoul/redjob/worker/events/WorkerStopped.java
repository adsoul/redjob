package com.adsoul.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.adsoul.redjob.worker.Worker;

/**
 * Worker stopped.
 */
public class WorkerStopped extends ApplicationEvent implements WorkerEvent {
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
   public WorkerStopped(Worker worker) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      this.worker = worker;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <W extends Worker> W getWorker() {
      return (W) worker;
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
