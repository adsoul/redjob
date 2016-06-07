package com.s24.redjob.channel.client;

import javax.annotation.PostConstruct;

import org.springframework.util.Assert;

import com.s24.redjob.queue.QueueDao;

/**
 * Default implementation of {@link AdminClient}.
 */
public class AdminClientImpl implements AdminClient {
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
   public void publish(String channel, Object job) {

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
