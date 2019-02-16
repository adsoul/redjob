package com.s24.redjob.channel.command;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.runner.JobRunner;
import com.s24.redjob.worker.runner.JobRunnerComponent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

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
    * Namespace.
    */
   private final String namespace;

   /**
    * Job.
    */
   private final StopJob job;

   /**
    * Constructor.
    *
    * @param execution
    *       Job execution.
    */
   public StopJobRunner(Execution execution) {
      Assert.notNull(execution, "Precondition violated: execution != null.");

      this.namespace = execution.getNamespace();
      this.job = execution.getJob();
   }

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = List.of();

   @Override
   public void run() {
      log.info("Stopping job {}.", job.getId());

      allWorkers.stream()
            .filter(worker -> matches(worker, job))
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
   private boolean matches(QueueWorker worker, StopJob job) {
      return worker.getNamespace().equals(namespace);
   }
}
