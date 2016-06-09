package com.s24.redjob.queue;

import javax.annotation.PostConstruct;

import org.springframework.util.Assert;

import com.s24.redjob.worker.AbstractQueueWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;

/**
 * Default implementation of {@link Worker} for queues based on a Redis list.
 */
public class QueueWorker extends AbstractQueueWorker {
   /**
    * Queue dao.
    */
   private QueueDao queueDao;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(queueDao, "Precondition violated: queueDao != null.");

      super.afterPropertiesSet();
   }

   @Override
   protected Execution doPollQueue(String queue) throws Throwable {
      return queueDao.pop(queue, name);
   }

   @Override
   protected void removeInflight(String queue) throws Throwable {
      queueDao.removeInflight(queue, name);
   }

   //
   // Injections.
   //

   /**
    * Queue dao.
    */
   public QueueDao getQueueDao() {
      return queueDao;
   }

   /**
    * Queue dao.
    */
   public void setQueueDao(QueueDao queueDao) {
      this.queueDao = queueDao;
   }
}
