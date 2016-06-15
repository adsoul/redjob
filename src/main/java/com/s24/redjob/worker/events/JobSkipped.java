package com.s24.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

/**
 * Worker has skipped a job due to a veto.
 */
public class JobSkipped extends ApplicationEvent implements JobFinished {
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
    * Job runner.
    */
   private final Runnable runner;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker.
    * @param queue
    *           Queue.
    * @param execution
    *           Job execution.
    * @param runner
    *           Job runner, may be null, if execution has been vetoed in process phase.
    */
   public JobSkipped(Worker worker, String queue, Execution execution, Runnable runner) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(execution, "Precondition violated: execution != null.");
      this.worker = worker;
      this.queue = queue;
      this.execution = execution;
      this.runner = runner;
   }

   @Override
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
   public <R> R getRunner() {
      return (R) runner;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof JobSkipped &&
            Objects.equals(worker, ((JobSkipped) o).worker) &&
            Objects.equals(queue, ((JobSkipped) o).queue) &&
            Objects.equals(execution, ((JobSkipped) o).execution) &&
            Objects.equals(runner, ((JobSkipped) o).runner);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
