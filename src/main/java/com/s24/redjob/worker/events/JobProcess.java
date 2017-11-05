package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;
import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Worker processes a job.
 */
public class JobProcess extends ApplicationEvent implements JobEvent {
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
    */
   public JobProcess(Worker worker, String queue, Execution execution) {
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
      return o instanceof JobProcess &&
            Objects.equals(worker, ((JobProcess) o).worker) &&
            Objects.equals(queue, ((JobProcess) o).queue) &&
            Objects.equals(execution, ((JobProcess) o).execution);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, execution);
   }
}
