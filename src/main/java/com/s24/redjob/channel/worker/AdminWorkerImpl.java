package com.s24.redjob.channel.worker;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.channel.ChannelDao;
import com.s24.redjob.queue.worker.JobRunnerFactory;

/**
 * Default implementation of {@link AdminWorker}.
 */
public class AdminWorkerImpl {
   /**
    * Log.
    */
   private static final Logger log = LoggerFactory.getLogger(AdminWorkerImpl.class);

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
    * Sequence for worker ids.
    */
   private static final AtomicInteger IDS = new AtomicInteger();

   /**
    * Worker id.
    */
   private int id;

   /**
    * Name of this worker.
    */
   private String name;

   /**
    * Factory for creating job runners.
    */
   private JobRunnerFactory jobRunnerFactory;

   @PostConstruct
   public void afterPropertiesSet() {
      Assert.notNull(channels, "Precondition violated: channels != null.");
      Assert.notNull(channelDao, "Precondition violated: channelDao != null.");
      Assert.notNull(listenerContainer, "Precondition violated: listenerContainer != null.");

      id = IDS.incrementAndGet();
      name = createName();

      listenerContainer.addMessageListener((message, pattern) -> {
         // Deserialize job.
            try {
               execute(null);
            } catch (Throwable throwable) {
               throwable.printStackTrace();
            }
         }, channels);
   }

   /**
    * Create name for this worker.
    */
   protected String createName() {
      return ManagementFactory.getRuntimeMXBean().getName() + ":" + id + ":" +
            StringUtils.collectionToCommaDelimitedString(channels);
   }

   /**
    * Execute job.
    *
    * @param payload
    *           Job.
    * @throws Throwable
    *            In case of errors.
    */
   protected void execute(Object payload) throws Throwable {
      if (payload == null) {
         log.error("AdminWorker {}: Missing payload.", name);
         throw new IllegalArgumentException("Missing payload.");
      }

      Runnable runner = jobRunnerFactory.runnerFor(payload);
      if (runner == null) {
         log.error("AdminWorker {}: Job {}: No runner found.", name, payload.getClass());
         throw new IllegalArgumentException("No runner found.");
      }

      log.info("AdminWorker {}: Job {}: Start.", name, payload.getClass());
      try {
         runner.run();
         log.info("AdminWorker {}: Job {}: Success.", name, payload.getClass());
      } catch (Throwable t) {
         log.error("AdminWorker {}: Job {}: Failed to execute.", name, payload.getClass(), t);
         throw new IllegalArgumentException("Failed to execute.", t);
      } finally {
         log.info("AdminWorker {}: Job {}: End.", name, payload.getClass());
      }
   }

   //
   // Injections.
   //

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
}
