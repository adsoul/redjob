package com.s24.redjob.worker;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.channel.ChannelDaoImpl;
import com.s24.redjob.channel.ChannelWorker;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class AbstractWorkerFactoryBean<W extends AbstractWorker> implements FactoryBean<W>, InitializingBean, ApplicationEventPublisherAware, DisposableBean {
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
   protected final W worker;

   /**
    * Constructor.
    *
    * @param worker
    *           Worker instance.
    */
   protected AbstractWorkerFactoryBean(W worker) {
      this.worker = worker;
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      workerDao.afterPropertiesSet();
      channelDao.afterPropertiesSet();

      worker.setWorkerDao(workerDao);
      worker.afterPropertiesSet();
   }

   @Override
   public void destroy() throws Exception {
      if (worker instanceof DisposableBean) {
         ((DisposableBean) worker).destroy();
      }
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
   public W getObject() throws Exception {
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
