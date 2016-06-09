package com.s24.redjob.channel;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.MDC;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.worker.AbstractWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;
import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * {@link Worker} for channels (admin jobs).
 */
public class ChannelWorker extends AbstractWorker {
   /**
    * Channels to listen to.
    */
   private List<Topic> channels;

   /**
    * Channel dao.
    */
   private ChannelDao channelDao;

   /**
    * Message listener container.
    */
   private RedisMessageListenerContainer listenerContainer;

   /**
    * Message listener.
    */
   private final MessageListener listener = this::receive;

   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(channels, "Precondition violated: channels != null.");
      Assert.notNull(channelDao, "Precondition violated: channelDao != null.");

      super.afterPropertiesSet();

      workerDao.start(name);
      eventBus.publishEvent(new WorkerStart(this));

      synchronized (listenerContainer) {
         listenerContainer.addMessageListener(listener, channels);
      }
   }

   @PreDestroy
   public void destroy() {
      synchronized (listenerContainer) {
         listenerContainer.removeMessageListener(listener);
      }

      eventBus.publishEvent(new WorkerStopped(this));
      workerDao.stop(name);

   }

   /**
    * Receive message from subscribed channel.
    *
    * @param message
    *           Message.
    * @param pattern
    *           Channel name pattern that let us receive the message.
    */
   private void receive(Message message, byte[] pattern) {
      try {
         MDC.put("worker", getName());
         String channel = channelDao.getChannel(message);
         MDC.put("queue", channel);
         Execution execution = channelDao.getExecution(message);
         MDC.put("execution", Long.toString(execution.getId()));
         MDC.put("job", execution.getJob().getClass().getSimpleName());

         process(channel, execution);

      } catch (Throwable t) {
         log.error("Uncaught exception in worker.", name, t);
         eventBus.publishEvent(new WorkerError(this, t));

      } finally {
         MDC.remove("job");
         MDC.remove("execution");
         MDC.remove("queue");
         MDC.remove("worker");
      }
   }

   /**
    * Create name for this worker.
    */
   protected String createName() {
      return super.createName() + ":" + StringUtils.collectionToCommaDelimitedString(channels);
   }

   //
   // Injections.
   //

   /**
    * Channels to listen to.
    */
   public List<Topic> getChannels() {
      return channels;
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(String... queues) {
      setChannels(Arrays.asList(queues));
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(List<String> channels) {
      this.channels = channels.stream().map(ChannelTopic::new).collect(Collectors.toList());
   }

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
    * Message listener container.
    */
   public RedisMessageListenerContainer getListenerContainer() {
      return listenerContainer;
   }

   /**
    * Message listener container.
    */
   public void setListenerContainer(RedisMessageListenerContainer listenerContainer) {
      this.listenerContainer = listenerContainer;
   }
}
