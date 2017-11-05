package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Worker executes a job. Can be vetoed. Veto leads to {@link JobSkipped} event.
 */
public class JobExecute extends ApplicationEvent implements JobEvent {
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
   private final Object runner;

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
    * @param execution
    *           Job execution.
    * @param runner
    *           Job runner.
    */
   public JobExecute(Worker worker, String queue, Execution execution, Object runner) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(execution, "Precondition violated: execution != null.");
      Assert.notNull(runner, "Precondition violated: runner != null.");
      this.worker = worker;
      this.queue = queue;
      this.execution = execution;
      this.runner = runner;
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
    * Job runner.
    */
   @SuppressWarnings("unchecked")
   public <R> R getRunner() {
      return (R) runner;
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
      return o instanceof JobExecute &&
            Objects.equals(worker, ((JobExecute) o).worker) &&
            Objects.equals(queue, ((JobExecute) o).queue) &&
            Objects.equals(execution, ((JobExecute) o).execution) &&
            Objects.equals(runner, ((JobExecute) o).runner);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
