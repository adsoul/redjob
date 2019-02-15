package com.s24.redjob.channel.command;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.runner.JobRunner;
import com.s24.redjob.worker.runner.JobRunnerComponent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * {@link JobRunner} for {@link StopJob} command.
 */
@JobRunnerComponent
public class StopJobRunner implements JobRunner<StopJob> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(StopJobRunner.class);

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = List.of();

   @Override
   public void execute(StopJob job) {
      log.info("Stopping job {}.", job.getId());

      allWorkers.stream()
            .filter(job::matches)
            .forEach(worker -> {
               try {
                  worker.stop(job.getId());
               } catch (Exception e) {
                  log.error("Failed to stop job {} of worker {}: {}.", job.getId(), worker.getName(), e.getMessage());
               }
            });
   }
}
