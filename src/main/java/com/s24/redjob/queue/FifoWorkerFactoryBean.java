package com.s24.redjob.queue;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.worker.AbstractWorkerFactoryBean;
import com.s24.redjob.worker.ExecutionRedisSerializer;

/**
 * {@link FactoryBean} for easy creation of a {@link FifoWorker}.
 */
public class FifoWorkerFactoryBean extends AbstractWorkerFactoryBean<FifoWorker> {
   /**
    * Queue dao.
    */
   private FifoDaoImpl fifoDao = new FifoDaoImpl();

   /**
    * Worker thread.
    */
   private Thread thread = null;

   /**
    * Constructor.
    */
   public FifoWorkerFactoryBean() {
      super(new FifoWorker());
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      fifoDao.afterPropertiesSet();

      worker.setFifoDao(fifoDao);

      super.afterPropertiesSet();

      thread = startThread();
   }

   /**
    * Create and start worker thread.
    */
   protected Thread startThread() {
      thread = new Thread(worker, worker.getName());
      thread.start();
      return thread;
   }

   //
   // Injections.
   //

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      super.setConnectionFactory(connectionFactory);
      fifoDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      super.setNamespace(namespace);
      fifoDao.setNamespace(namespace);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return fifoDao.getExecutions();
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      fifoDao.setExecutions(executions);
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
