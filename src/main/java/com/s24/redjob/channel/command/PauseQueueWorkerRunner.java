package com.s24.redjob.channel.command;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.s24.redjob.channel.WorkerAware;
import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerComponent;

/**
 * {@link JobRunner} for {@link PauseQueueWorker} command.
 */
@JobRunnerComponent
public class PauseQueueWorkerRunner implements JobRunner<PauseQueueWorker>, WorkerAware {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(PauseQueueWorkerRunner.class);

   /**
    * Workers to pause.
    */
   private List<QueueWorker> workers;

   @Override
   public void execute(PauseQueueWorker job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      boolean pause = job.isPause();
      List<QueueWorker> selectedWorkers = workers.stream().filter(job::matches).collect(toList());

      log.info("{} {} workers.", pause? "Pausing" : "Unpausing", selectedWorkers.size());
      for (QueueWorker worker : selectedWorkers) {
         try {
            worker.pause(pause);
         } catch (Exception e) {
            log.error("Failed to {} worker {}: {}.", pause ? "pause" : "unpause", worker.getName(), e.getMessage());
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
