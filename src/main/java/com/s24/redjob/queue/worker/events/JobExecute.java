package com.s24.redjob.queue.worker.events;

import java.util.Objects;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.s24.redjob.queue.worker.Worker;

/**
 * Worker executes a job.
 */
public class JobExecute extends ApplicationEvent {
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
    * @param job
    *           Job.
    * @param runner
    *           Job runner.
    */
   public JobExecute(Worker worker, String queue, Object job, Runnable runner) {
      super(worker);
      Assert.notNull(worker, "Precondition violated: worker != null.");
      Assert.hasLength(queue, "Precondition violated: queue has length.");
      Assert.notNull(job, "Precondition violated: job != null.");
      Assert.notNull(runner, "Precondition violated: runner != null.");
      this.worker = worker;
      this.queue = queue;
      this.job = job;
      this.runner = runner;
   }

   /**
    * Worker.
    */
   public Worker getWorker() {
      return worker;
   }

   /**
    * Queue.
    */
   public String getQueue() {
      return queue;
   }

   /**
    * Job.
    */
   public Object getJob() {
      return job;
   }

   /**
    * Job runner.
    */
   public Runnable getRunner() {
      return runner;
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
            Objects.equals(job, ((JobExecute) o).job) &&
            Objects.equals(runner, ((JobExecute) o).runner);
   }

   @Override
   public int hashCode() {
      return Objects.hash(worker, queue, job, runner);
   }
}
