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
 * {@link JobRunner} for {@link ShutdownQueueWorker} command.
 */
@JobRunnerComponent
public class ShutdownQueueWorkerRunner implements JobRunner<ShutdownQueueWorker> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(ShutdownQueueWorkerRunner.class);

   /**
    * Namespace.
    */
   private final String namespace;

   /**
    * Job.
    */
   private final ShutdownQueueWorker job;

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = List.of();

   /**
    * Constructor.
    *
    * @param execution
    *       Job execution.
    */
   public ShutdownQueueWorkerRunner(Execution execution) {
      Assert.notNull(execution, "Precondition violated: execution != null.");

      this.namespace = execution.getNamespace();
      this.job = execution.getJob();
   }

   @Override
   public void run() {
      allWorkers.stream()
            .filter(worker -> matches(worker, job))
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
   private boolean matches(QueueWorker worker, ShutdownQueueWorker job) {
      return worker.getNamespace().equals(namespace);
   }
}
