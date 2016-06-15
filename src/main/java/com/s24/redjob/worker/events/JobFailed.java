package com.s24.redjob.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.s24.redjob.worker.Worker;

/**
 * Worker failed to execute a job.
 */
public class JobFailed extends ApplicationEvent implements JobFinished {
   /**
    * Worker.
    */
   private final Worker worker;

   /**
    * Queue.
    */
   private final String queue;

   /**
    * Job.
    */
   private final Object job;

   /**
    * Job runner.
    */
   private final Runnable runner;

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
    * @param job
    *           Job.
    * @param runner
    *           Job runner.
    * @param cause
    *           Cause of failure.
    */
   public JobFailed(Worker worker, String queue, Object job, Runnable runner, Throwable cause) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(job, "Precondition violated: job != null.");
      Assert.notNull(runner, "Precondition violated: runner != null.");
      Assert.notNull(cause, "Precondition violated: cause != null.");
      this.worker = worker;
      this.queue = queue;
      this.job = job;
      this.runner = runner;
      this.cause = cause;
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
   public <J> J getJob() {
      return (J) job;
   }

   @Override
   public <R> R getRunner() {
      return (R) runner;
   }

   /**
    * Cause of failure.
    */
   public Throwable getCause() {
      return cause;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof JobFailed &&
            Objects.equals(worker, ((JobFailed) o).worker) &&
            Objects.equals(queue, ((JobFailed) o).queue) &&
            Objects.equals(job, ((JobFailed) o).job) &&
            Objects.equals(runner, ((JobFailed) o).runner) &&
            Objects.equals(cause, ((JobFailed) o).cause);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, job);
   }
}
