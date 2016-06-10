package com.s24.redjob.channel;

import java.util.List;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.worker.AbstractWorker;
import com.s24.redjob.worker.ExecutionRedisSerializer;
import com.s24.redjob.worker.JobRunnerFactory;
import com.s24.redjob.worker.WorkerDaoImpl;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class ChannelWorkerFactoryBean implements FactoryBean<ChannelWorker>, InitializingBean, ApplicationEventPublisherAware, DisposableBean {
   /**
    * Worker dao.
    */
   private WorkerDaoImpl workerDao = new WorkerDaoImpl();

   /**
    * Channel dao.
    */
   private ChannelDaoImpl channelDao = new ChannelDaoImpl();

   /**
    * The instance.
    */
   private ChannelWorker worker = new ChannelWorker();

   @Override
   public void afterPropertiesSet() throws Exception {
      workerDao.afterPropertiesSet();
      channelDao.afterPropertiesSet();

      worker.setWorkerDao(workerDao);
      worker.setChannelDao(channelDao);
      worker.afterPropertiesSet();
   }

   @Override
   public void destroy() throws Exception {
      worker.destroy();
   }

   @Override
   public boolean isSingleton() {
      return true;
   }

   @Override
   public Class<ChannelWorker> getObjectType() {
      return ChannelWorker.class;
   }

   @Override
   public ChannelWorker getObject() throws Exception {
      return worker;
   }

   //
   // Injections.
   //

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public RedisConnectionFactory getConnectionFactory() {
      return workerDao.getConnectionFactory();
   }

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      workerDao.setConnectionFactory(connectionFactory);
      channelDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public String getNamespace() {
      return workerDao.getNamespace();
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      workerDao.setNamespace(namespace);
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
    * Factory for creating job runners.
    */
   public JobRunnerFactory getJobRunnerFactory() {
      return worker.getJobRunnerFactory();
   }

   /**
    * Factory for creating job runners.
    */
   public void setJobRunnerFactory(JobRunnerFactory jobRunnerFactory) {
      worker.setJobRunnerFactory(jobRunnerFactory);
   }

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * AbstractWorker#DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public long getEmptyQueuesSleepMillis() {
      return worker.getEmptyQueuesSleepMillis();
   }

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * AbstractWorker#DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public void setEmptyQueuesSleepMillis(long emptyQueuesSleepMillis) {
      worker.setEmptyQueuesSleepMillis(emptyQueuesSleepMillis);
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher eventBus) {
      this.worker.setApplicationEventPublisher(eventBus);
   }
}
