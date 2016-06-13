package com.s24.redjob.queue;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.worker.AbstractWorkerFactoryBean;
import com.s24.redjob.worker.ExecutionRedisSerializer;

/**
 * {@link FactoryBean} for easy creation of a {@link QueueWorker}.
 */
public class QueueWorkerFactoryBean extends AbstractWorkerFactoryBean<QueueWorker> {
   /**
    * Queue dao.
    */
   private QueueDaoImpl queueDao = new QueueDaoImpl();

   /**
    * Constructor.
    */
   public QueueWorkerFactoryBean() {
      super(new QueueWorker());
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      queueDao.afterPropertiesSet();

      worker.setQueueDao(queueDao);

      super.afterPropertiesSet();
   }

   //
   // Injections.
   //

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      super.setConnectionFactory(connectionFactory);
      queueDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      super.setNamespace(namespace);
      queueDao.setNamespace(namespace);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return queueDao.getExecutions();
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      queueDao.setExecutions(executions);
   }

   /**
    * Queues to listen to.
    */
   public List<String> getQueues() {
      return worker.getQueues();
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(String... queues) {
      worker.setQueues(queues);
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(List<String> queues) {
      worker.setQueues(queues);
   }
}
