package com.s24.redjob.channel.command;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.runner.JobRunner;
import com.s24.redjob.worker.runner.JobRunnerComponent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

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
   public void run(Execution execution) {
      PauseQueueWorker job = execution.getJob();
      boolean pause = job.isPause();
      allWorkers.stream()
            .filter(worker -> matches(worker, execution.getNamespace(), job))
            .forEach(worker -> {
               try {
                  log.info("{} worker {}.", pause ? "Pausing" : "Unpausing", worker.getName());
                  worker.pause(pause);
               } catch (Exception e) {
                  log.error("Failed to {} worker {}: {}.", pause ? "pause" : "unpause", worker.getName(), e.getMessage());
               }
            });
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(QueueWorker worker, String namespace, PauseQueueWorker job) {
      return worker.getNamespace().equals(namespace) &&
            (job.getQueues().isEmpty() || worker.getQueues().stream().anyMatch(job.getQueues()::contains));
   }
}
