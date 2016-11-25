package com.s24.redjob.channel.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.s24.redjob.channel.WorkersAware;
import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerComponent;

/**
 * {@link JobRunner} for {@link ShutdownQueueWorker} command.
 */
@JobRunnerComponent
public class ShutdownQueueWorkerRunner implements JobRunner<ShutdownQueueWorker>, WorkersAware {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(ShutdownQueueWorkerRunner.class);

   /**
    * Workers to shutdown.
    */
   private List<QueueWorker> workers;

   @Override
   public void execute(ShutdownQueueWorker job) {
      log.info("Shutting down {} workers.", workers.size());

      for (QueueWorker worker : workers) {
         try {
            worker.stop();
         } catch (Exception e) {
            log.error("Failed to stop worker {}: {}.", worker.getName(), e.getMessage());
         }
      }
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
