package com.adsoul.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.adsoul.redjob.worker.Worker;

/**
 * Worker got an exception.
 */
public class WorkerError extends ApplicationEvent implements WorkerEvent {
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
    * @param worker
    *           Worker.
    */
   public WorkerError(Worker worker, Throwable throwable) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.notNull(throwable, "Precondition violated: throwable != null.");
      this.worker = worker;
      this.throwable = throwable;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <W extends Worker> W getWorker() {
      return (W) worker;
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
