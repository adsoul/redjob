package com.s24.redjob.channel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.MDC;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.AbstractWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;
import com.s24.redjob.worker.WorkerState;
import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * {@link Worker} for channels (admin jobs).
 */
public class ChannelWorker extends AbstractWorker {
   /**
    * Channel dao.
    */
   private ChannelDao channelDao;

   /**
    * Channels to listen to.
    */
   private List<String> channels;

   /**
    * Message listener container.
    */
   private RedisMessageListenerContainer listenerContainer;

   /**
    * Message listener.
    */
   private final MessageListener listener = this::receive;

   /**
    * Monitor active jobs.
    */
   private final ReadWriteLock active = new ReentrantReadWriteLock();

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    */
   private List<QueueWorker> workers;

   @Override
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(channels, "Precondition violated: channels != null.");
      Assert.notNull(channelDao, "Precondition violated: channelDao != null.");

      super.afterPropertiesSet();
   }

   @Override
   public void start() {
      log.info("Starting worker {}.", getName());
      log.info("Listening to channels {}.", StringUtils.collectionToCommaDelimitedString(channels));
      List<Topic> topics = channels.stream().map(channelDao::getTopic).collect(Collectors.toList());
      synchronized (listenerContainer) {
         listenerContainer.addMessageListener(listener, topics);
      }

      state = new WorkerState();
      setWorkerState(WorkerState.RUNNING);
      eventBus.publishEvent(new WorkerStart(this));
   }

   @Override
   public void stop() {
      super.stop();

      synchronized (listenerContainer) {
         listenerContainer.removeMessageListener(listener);
      }

      // Wait for jobs to finish.
      try {
         active.writeLock().lock();
         eventBus.publishEvent(new WorkerStopped(this));
         workerDao.stop(name);
      } finally {
         active.writeLock().unlock();
      }
   }

   @Override
   @PreDestroy
   public void destroy() {
      stop();
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
         active.readLock().lock();

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
         active.readLock().unlock();

         MDC.remove("job");
         MDC.remove("execution");
         MDC.remove("queue");
         MDC.remove("worker");
      }
   }

   /**
    * Create name for this worker.
    */
   @Override
   protected String createName() {
      return super.createName() + ":" + StringUtils.collectionToCommaDelimitedString(channels);
   }

   @Override
   protected void prepareRunner(Object runner) {
      super.prepareRunner(runner);
      if (runner instanceof WorkersAware) {
         ((WorkersAware) runner).setWorkers(workers);
      }
   }

   //
   // Injections.
   //

   /**
    * Channels to listen to.
    */
   public List<String> getChannels() {
      return channels;
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(String... channels) {
      setChannels(Arrays.asList(channels));
   }

   /**
    * Channels to listen to.
    */
   public void setChannels(List<String> channels) {
      this.channels = channels;
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

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    */
   public List<QueueWorker> getWorkers() {
      return workers;
   }

   /**
    * {@link QueueWorker}s this worker should execute commands for.
    */
   public void setWorkers(List<QueueWorker> workers) {
      this.workers = workers;
   }
}
