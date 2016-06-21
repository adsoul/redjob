package com.s24.redjob.channel;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.AbstractWorkerFactoryBean;
import com.s24.redjob.worker.ExecutionRedisSerializer;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class ChannelWorkerFactoryBean extends AbstractWorkerFactoryBean<ChannelWorker> {
   /**
    * Channel dao.
    */
   private ChannelDaoImpl channelDao = new ChannelDaoImpl();

   /**
    * All {@link QueueWorker}s.
    */
   @Autowired(required = false)
   private List<QueueWorker> allWorkers = Collections.emptyList();

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    * Defaults to all {@link QueueWorker}s of the namespace.
    */
   private List<QueueWorker> workers;

   /**
    * Constructor.
    */
   public ChannelWorkerFactoryBean() {
      super(new ChannelWorker());
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      // All workers of this namespace, if workers are not injected.
      if (workers == null) {
         String namespace = getNamespace();
         workers = allWorkers.stream()
               .filter(worker -> worker.getNamespace().equals(namespace))
               .collect(Collectors.toList());
      }

      channelDao.afterPropertiesSet();

      worker.setChannelDao(channelDao);
      worker.setWorkers(workers);

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
      channelDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      super.setNamespace(namespace);
      channelDao.setNamespace(namespace);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return channelDao.getExecutions();
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      channelDao.setExecutions(executions);
   }

   /**
    * Channels to listen to.
    */
   public List<String> getChannels() {
      return worker.getChannels();
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(String... channels) {
      worker.setChannels(channels);
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(List<String> channels) {
      worker.setChannels(channels);
   }

   /**
    * Message listener container.
    */
   public RedisMessageListenerContainer getListenerContainer() {
      return worker.getListenerContainer();
   }

   /**
    * Message listener container.
    */
   public void setListenerContainer(RedisMessageListenerContainer listenerContainer) {
      worker.setListenerContainer(listenerContainer);
   }

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    * Defaults to all {@link QueueWorker}s of the namespace.
    */
   public List<QueueWorker> getWorkers() {
      return workers;
   }

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    * Defaults to all {@link QueueWorker}s of the namespace.
    */
   public void setWorkers(List<QueueWorker> workers) {
      this.workers = workers;
   }
}
