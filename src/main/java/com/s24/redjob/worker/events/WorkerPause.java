package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker gets paused.
 */
public class WorkerPause extends ApplicationEvent implements WorkerEvent {
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
   public WorkerPause(Worker worker) {
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
      return o instanceof WorkerPause &&
            Objects.equals(worker, ((WorkerPause) o).worker);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker);
   }
}
