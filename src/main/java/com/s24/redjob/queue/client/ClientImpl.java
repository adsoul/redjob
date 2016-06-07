package com.s24.redjob.queue.client;

import javax.annotation.PostConstruct;

import org.springframework.util.Assert;

import com.s24.redjob.queue.QueueDao;

/**
 * Default implementation of {@link Client}.
 */
public class ClientImpl implements Client {
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
   }

   @Override
   public long enqueue(String queue, Object payload, boolean front) {
      return queueDao.enqueue(queue, payload, front);
   }

   @Override
   public void dequeue(String queue, long id) {
      queueDao.dequeue(queue, id);
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
