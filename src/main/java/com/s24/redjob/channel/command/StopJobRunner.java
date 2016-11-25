package com.s24.redjob.channel.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.s24.redjob.channel.WorkersAware;
import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerComponent;

/**
 * {@link JobRunner} for {@link StopJob} command.
 */
@JobRunnerComponent
public class StopJobRunner implements JobRunner<StopJob>, WorkersAware {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(StopJobRunner.class);

   /**
    * Workers to check for running jobs.
    */
   private List<QueueWorker> workers;

   @Override
   public void execute(StopJob job) {
      log.info("Stopping job {}.", job.getId());

      for (QueueWorker worker : workers) {
         try {
            worker.stop(job.getId());
         } catch (Exception e) {
            log.error("Failed to stop job {} of worker {}: {}.", job.getId(), worker.getName(), e.getMessage());
         }
      }
   }

   //
   // Injections.
   //

   /**
    * Workers to check for running jobs.
    */
   public List<QueueWorker> getWorkers() {
      return workers;
   }

   @Override
   public void setWorkers(List<QueueWorker> workers) {
      this.workers = workers;
   }
}
