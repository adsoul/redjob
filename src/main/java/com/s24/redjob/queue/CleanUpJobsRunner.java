package com.s24.redjob.queue;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.s24.redjob.worker.JobRunner;
import com.s24.redjob.worker.JobRunnerComponent;

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
    * All DAOs.
    */
   @Autowired(required = false)
   private List<FifoDao> fifoDaos = emptyList();

   @Override
   public void execute(CleanUpJobs job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      List<FifoDao> selectedDaos = fifoDaos.stream()
            .filter(job::matches)
            .collect(toList());

      log.info("Cleaning up jobs of {} namespaces.", selectedDaos.size());
      for (FifoDao fifoDao : selectedDaos) {
         try {
            int deletedJobs = fifoDao.cleanUp();
            log.info("Deleted {} jobs of namespace {}.", deletedJobs, fifoDao.getNamespace());
         } catch (Exception e) {
            log.error("Failed to clean up jobs of namespace {}: {}.", fifoDao.getNamespace(), e.getMessage());
         }
      }
      log.info("Cleaned up jobs of {} namespaces.", selectedDaos.size());
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
