package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker failed to execute a job.
 */
public class JobFailure extends ApplicationEvent implements JobFinished {
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
    * Cause of failure.
    */
   private final Throwable cause;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker.
    * @param queue
    *           Queue.
    * @param execution
    *           Job execution.
    * @param cause
    *           Cause of failure.
    */
   public JobFailure(Worker worker, String queue, Execution execution, Throwable cause) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(execution, "Precondition violated: execution != null.");
      Assert.notNull(cause, "Precondition violated: cause != null.");
      this.worker = worker;
      this.queue = queue;
      this.execution = execution;
      this.cause = cause;
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

   /**
    * Cause of failure.
    */
   public Throwable getCause() {
      return cause;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof JobFailure &&
            Objects.equals(worker, ((JobFailure) o).worker) &&
            Objects.equals(queue, ((JobFailure) o).queue) &&
            Objects.equals(execution, ((JobFailure) o).execution) &&
            Objects.equals(cause, ((JobFailure) o).cause);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
