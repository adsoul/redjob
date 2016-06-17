package com.s24.redjob.channel.admin;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.s24.redjob.worker.AbstractQueueWorker;
import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.Worker;

/**
 * {@link JobRunner} for {@link ShutdownQueueWorker} command.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ShutdownQueueWorkerRunner implements JobRunner<ShutdownQueueWorker> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(ShutdownQueueWorkerRunner.class);

   /**
    * Workers to shutdown, if not set, defaults to all workers in the application context.
    */
   @Autowired
   private List<AbstractQueueWorker> workers;

   @Override
   public void execute(ShutdownQueueWorker job) {
      log.info("Shutting down {} workers.", workers.size());

      for (Worker worker : workers) {
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
    * Workers to shutdown, if not set, defaults to all workers in the application context.
    */
   public List<AbstractQueueWorker> getWorkers() {
      return workers;
   }

   /**
    * Workers to shutdown, if not set, defaults to all workers in the application context.
    */
   public void setWorkers(List<AbstractQueueWorker> workers) {
      this.workers = workers;
   }
}
