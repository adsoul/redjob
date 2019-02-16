package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Worker starts a job. Cannot be vetoed. Last event directly before the job runner will be called.
 */
public class JobStart extends ApplicationEvent implements JobEvent {
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
   public JobStart(Worker worker, String queue, Execution execution) {
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
      return o instanceof JobStart &&
            Objects.equals(worker, ((JobStart) o).worker) &&
            Objects.equals(queue, ((JobStart) o).queue) &&
            Objects.equals(execution, ((JobStart) o).execution);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
