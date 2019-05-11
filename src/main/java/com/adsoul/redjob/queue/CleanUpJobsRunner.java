package com.adsoul.redjob.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.runner.JobRunner;
import com.adsoul.redjob.worker.runner.JobRunnerComponent;

import static java.util.Collections.emptyList;

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
   public void run(Execution execution) {
      CleanUpJobs job = execution.getJob();

      log.info("Cleaning up jobs.");
      fifoDaos.stream()
            .filter(fifoDao -> matches(fifoDao, execution.getNamespace(), job))
            .forEach(fifoDao -> {
               try {
                  int deletedJobs = fifoDao.cleanUp();
                  log.info("Deleted {} jobs.", deletedJobs);
               } catch (Exception e) {
                  log.error("Failed to clean up jobs: {}.", e.getMessage());
               }
            });
      log.info("Cleaned up jobs.");
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   private boolean matches(FifoDao fifoDao, String namespace, CleanUpJobs job) {
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
