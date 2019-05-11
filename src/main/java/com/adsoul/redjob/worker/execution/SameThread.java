package com.adsoul.redjob.worker.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.runner.JobRunner;
import com.adsoul.redjob.worker.runner.JobRunnerFactory;

/**
 * Execute {@link JobRunner}s in the worker thread.
 */
public class SameThread implements ExecutionStrategy {
   /**
    * Log.
    */
   private static final Logger log = LoggerFactory.getLogger(SameThread.class);

   /**
    * Factory for creating job runners.
    */
   private final JobRunnerFactory jobRunnerFactory;

   /**
    * Constructor.
    *
    * @param jobRunnerFactory
    *       Factory for creating job runners.
    */
   public SameThread(JobRunnerFactory jobRunnerFactory) {
      this.jobRunnerFactory = jobRunnerFactory;
   }

   @Override
   public void execute(String queue, Execution execution) {
      Runnable runner = jobRunnerFactory.runnerFor(execution);
      if (runner == null) {
         log.error("No job runner found.", execution.getWorker());
         throw new IllegalArgumentException("No job runner found.");
      }

      runner.run();
   }
}
