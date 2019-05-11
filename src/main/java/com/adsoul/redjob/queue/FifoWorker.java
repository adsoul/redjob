package com.adsoul.redjob.queue;

import javax.annotation.PostConstruct;

import org.springframework.util.Assert;

import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.Worker;

/**
 * Default implementation of {@link Worker} for queues based on a Redis list.
 */
public class FifoWorker extends AbstractQueueWorker {
   /**
    * Queue dao.
    */
   private FifoDao fifoDao;

   /**
    * Init.
    */
   @Override
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(fifoDao, "Precondition violated: fifoDao != null.");

      super.afterPropertiesSet();
   }

   @Override
   protected Execution get(long id) throws Throwable {
      return fifoDao.get(id);
   }

   @Override
   protected Execution doPollQueue(String queue) throws Throwable {
      return fifoDao.pop(queue, name);
   }

   @Override
   protected void removeInflight(String queue) throws Throwable {
      fifoDao.removeInflight(queue, name);
   }

   @Override
   protected void restoreInflight(String queue) throws Throwable {
      fifoDao.restoreInflight(queue, name);
   }

   @Override
   public void update(Execution execution) {
      fifoDao.update(execution);
   }

   //
   // Injections.
   //

   /**
    * Queue dao.
    */
   public FifoDao getFifoDao() {
      return fifoDao;
   }

   /**
    * Queue dao.
    */
   public void setFifoDao(FifoDao fifoDao) {
      this.fifoDao = fifoDao;
   }
}
