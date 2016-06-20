package com.s24.redjob.channel.admin;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.s24.redjob.worker.AbstractQueueWorker;
import com.s24.redjob.worker.JobRunner;

/**
 * {@link JobRunner} for {@link StopJob} command.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StopJobRunner implements JobRunner<StopJob> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(StopJobRunner.class);

   /**
    * All workers.
    */
   @Autowired
   private List<AbstractQueueWorker> workers;

   @Override
   public void execute(StopJob job) {
      log.info("Stopping job {}.", job.getId());

      for (AbstractQueueWorker worker : workers) {
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
    * Workers to shutdown, if not set, defaults to all workers in the application context.
    */
   public List<AbstractQueueWorker> getWorkers() {
      return workers;
   }

   /**
    * Workers to shutdown, if not set, defaults to all workers in the application context.
    */
   public void setWorkers(List<AbstractQueueWorker> workers) {
      this.workers = workers;
   }
}
