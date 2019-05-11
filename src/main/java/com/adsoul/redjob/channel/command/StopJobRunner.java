package com.adsoul.redjob.channel.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.adsoul.redjob.queue.QueueWorker;
import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.runner.JobRunner;
import com.adsoul.redjob.worker.runner.JobRunnerComponent;

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
   public void run(Execution execution) {
      StopJob job = execution.getJob();
      log.info("Stopping job {}.", job.getId());

      allWorkers.stream()
            .filter(worker -> matches(worker, execution.getNamespace(), job))
            .forEach(worker -> {
               try {
                  worker.stop(job.getId());
               } catch (Exception e) {
                  log.error("Failed to stop job {} of worker {}: {}.", job.getId(), worker.getName(), e.getMessage());
               }
            });
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(QueueWorker worker, String namespace, StopJob job) {
      return worker.getNamespace().equals(namespace);
   }
}
