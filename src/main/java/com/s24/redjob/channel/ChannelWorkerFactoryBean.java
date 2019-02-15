package com.s24.redjob.channel;

import com.s24.redjob.worker.AbstractWorkerFactoryBean;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class ChannelWorkerFactoryBean extends AbstractWorkerFactoryBean<ChannelWorker> {
   /**
    * Channel dao.
    */
   private ChannelDao channelDao;

   /**
    * Constructor.
    */
   public ChannelWorkerFactoryBean() {
      super(new ChannelWorker());
   }

   //
   // Injections.
   //

   /**
    * Channel dao.
    */
   public ChannelDao getChannelDao() {
      return worker.getChannelDao();
   }

   /**
    * Channel dao.
    */
   public void setChannelDao(ChannelDao channelDao) {
      this.worker.setChannelDao(channelDao);
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
}
