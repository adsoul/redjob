package com.s24.redjob.channel.command;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.runner.JobRunner;
import com.s24.redjob.worker.runner.JobRunnerComponent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

/**
 * {@link JobRunner} for {@link PauseQueueWorker} command.
 */
@JobRunnerComponent
public class PauseQueueWorkerRunner implements JobRunner<PauseQueueWorker> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(PauseQueueWorkerRunner.class);

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = List.of();

   @Override
   public void execute(PauseQueueWorker job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      boolean pause = job.isPause();
      allWorkers.stream()
            .filter(worker -> matches(worker, job))
            .forEach(worker -> {
               try {
                  log.info("{} worker {}.", pause? "Pausing" : "Unpausing", worker.getName());
                  worker.pause(pause);
               } catch (Exception e) {
                  log.error("Failed to {} worker {}: {}.", pause ? "pause" : "unpause", worker.getName(), e.getMessage());
               }
            });
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(QueueWorker worker, PauseQueueWorker job) {
      // worker.getNamespace().equals(job.getNamespace()) &&
      return job.getQueues().isEmpty() || worker.getQueues().stream().anyMatch(job.getQueues()::contains);
   }
}
