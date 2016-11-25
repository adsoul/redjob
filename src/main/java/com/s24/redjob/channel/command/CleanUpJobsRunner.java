package com.s24.redjob.channel.command;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.s24.redjob.channel.WorkersAware;
import com.s24.redjob.queue.FifoWorker;
import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerComponent;

/**
 * {@link JobRunner} for {@link CleanUpJobs} command.
 */
@JobRunnerComponent
public class CleanUpJobsRunner implements JobRunner<CleanUpJobs>, WorkersAware {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(CleanUpJobsRunner.class);

   /**
    * Workers to pause.
    */
   private List<QueueWorker> workers;

   @Override
   public void execute(CleanUpJobs job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      List<FifoWorker> selectedWorkers = workers.stream()
            .filter(FifoWorker.class::isInstance)
            .map(FifoWorker.class::cast)
            .filter(job::matches)
            .collect(toList());

      log.info("Cleaning up jobs of {} workers.", selectedWorkers.size());
      for (FifoWorker worker : selectedWorkers) {
         try {
            int deletedJobs = worker.getFifoDao().cleanUp();
            log.info("Cleaned up {} jobs.", deletedJobs);
         } catch (Exception e) {
            log.error("Failed to clean up worker {}: {}.", worker.getName(), e.getMessage());
         }
      }
      log.info("Cleaned up jobs of {} workers.", selectedWorkers.size());
   }

   //
   // Injections.
   //

   /**
    * Workers to shutdown.
    */
   public List<QueueWorker> getWorkers() {
      return workers;
   }

   @Override
   public void setWorkers(List<QueueWorker> workers) {
      this.workers = workers;
   }
}
