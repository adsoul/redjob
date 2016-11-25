package com.s24.redjob.channel;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.AbstractWorkerFactoryBean;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class ChannelWorkerFactoryBean extends AbstractWorkerFactoryBean<ChannelWorker> {
   /**
    * Channel dao.
    */
   private ChannelDao channelDao;

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
         String namespace = channelDao.getNamespace();
         workers = allWorkers.stream()
               .filter(worker -> worker.getNamespace().equals(namespace))
               .collect(Collectors.toList());
      }

      worker.setChannelDao(channelDao);
      worker.setWorkers(workers);

      super.afterPropertiesSet();
   }

   //
   // Injections.
   //

   /**
    * Channel dao.
    */
   public ChannelDao getChannelDao() {
      return channelDao;
   }

   /**
    * Channel dao.
    */
   public void setChannelDao(ChannelDao channelDao) {
      this.channelDao = channelDao;
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
