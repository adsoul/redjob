package com.s24.redjob.queue;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.runner.JobRunner;
import com.s24.redjob.worker.runner.JobRunnerComponent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * {@link JobRunner} for {@link CleanUpJobs} command.
 */
@JobRunnerComponent
public class CleanUpJobsRunner implements JobRunner<CleanUpJobs> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(CleanUpJobsRunner.class);

   /**
    * Namespace.
    */
   private final String namespace;

   /**
    * Job.
    */
   private final CleanUpJobs job;

   /**
    * All DAOs.
    */
   @Autowired(required = false)
   private List<FifoDao> fifoDaos = emptyList();

   /**
    * Constructor.
    *
    * @param execution
    *       Job execution.
    */
   public CleanUpJobsRunner(Execution execution) {
      Assert.notNull(execution, "Precondition violated: execution != null.");

      this.namespace = execution.getNamespace();
      this.job = execution.getJob();
   }

   @Override
   public void run() {
      List<FifoDao> selectedDaos = fifoDaos.stream()
            .filter(fifoDao -> matches(fifoDao, job))
            .collect(toList());

      log.info("Cleaning up jobs of {} namespaces.", selectedDaos.size());
      for (FifoDao fifoDao : selectedDaos) {
         try {
            int deletedJobs = fifoDao.cleanUp();
            log.info("Deleted {} jobs of namespace {}.", deletedJobs, namespace);
         } catch (Exception e) {
            log.error("Failed to clean up jobs of namespace {}: {}.", namespace, e.getMessage());
         }
      }
      log.info("Cleaned up jobs of {} namespaces.", selectedDaos.size());
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(FifoDao fifoDao, CleanUpJobs job) {
      return fifoDao.getNamespace().equals(namespace);
   }

   //
   // Injections.
   //

   /**
    * All DAOs.
    */
   public List<FifoDao> getFifoDaos() {
      return fifoDaos;
   }

   /**
    * All DAOs.
    */
   public void setFifoDaos(List<FifoDao> fifoDaos) {
      this.fifoDaos = fifoDaos;
   }
}
