package com.s24.redjob.queue.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.s24.redjob.queue.worker.Worker;

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
    * Job.
    */
   private final Object job;

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
    * @param job
    *           Job.
    * @param runner
    *           Job runner, may be null, if execution has been vetoed in process phase.
    */
   public JobSkipped(Worker worker, String queue, Object job, Runnable runner) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(job, "Precondition violated: job != null.");
      this.worker = worker;
      this.queue = queue;
      this.job = job;
      this.runner = runner;
   }

   @Override
   public Worker getWorker() {
      return worker;
   }

   @Override
   public String getQueue() {
      return queue;
   }

   @Override
   public Object getJob() {
      return job;
   }

   @Override
   public Runnable getRunner() {
      return runner;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof JobSkipped &&
            Objects.equals(worker, ((JobSkipped) o).worker) &&
            Objects.equals(queue, ((JobSkipped) o).queue) &&
            Objects.equals(job, ((JobSkipped) o).job) &&
            Objects.equals(runner, ((JobSkipped) o).runner);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, job, runner);
   }
}
