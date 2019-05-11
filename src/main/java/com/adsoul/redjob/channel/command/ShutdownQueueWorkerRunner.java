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
 * {@link JobRunner} for {@link ShutdownQueueWorker} command.
 */
@JobRunnerComponent
public class ShutdownQueueWorkerRunner implements JobRunner<ShutdownQueueWorker> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(ShutdownQueueWorkerRunner.class);

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = List.of();

   @Override
   public void run(Execution execution) {
      allWorkers.stream()
            .filter(worker -> matches(worker, execution.getNamespace(), execution.getJob()))
            .forEach(worker -> {
               try {
                  log.info("Shutting down worker {}.", worker.getName());
                  worker.stop();
               } catch (Exception e) {
                  log.error("Failed to stop worker {}: {}.", worker.getName(), e.getMessage());
               }
            });
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(QueueWorker worker, String namespace, ShutdownQueueWorker job) {
      return worker.getNamespace().equals(namespace);
   }
}
