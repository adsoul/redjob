package com.s24.redjob.worker;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.channel.ChannelWorker;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public class AbstractWorkerFactoryBean<W extends AbstractWorker>
      implements SmartFactoryBean<W>, InitializingBean, DisposableBean, ApplicationEventPublisherAware {
   /**
    * Worker dao.
    */
   private WorkerDaoImpl workerDao = new WorkerDaoImpl();

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

      worker.setWorkerDao(workerDao);
      worker.afterPropertiesSet();
   }

   @Override
   public void destroy() throws Exception {
      worker.destroy();
   }

   @Override
   public boolean isEagerInit() {
      return true;
   }

   @Override
   public boolean isPrototype() {
      return false;
   }

   @Override
   public boolean isSingleton() {
      return true;
   }

   @Override
   public Class<W> getObjectType() {
      return (Class<W>) worker.getClass();
   }

   @Override
   public W getObject() throws Exception {
      return worker;
   }

   //
   // Injections.
   //

   /**
    * Name of this worker. Defaults to a generated unique name.
    */
   public String getName() {
      return worker.getName();
   }

   /**
    * Name of this worker. Defaults to a generated unique name.
    */
   public void setName(String name) {
      this.worker.setName(name);
   }

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
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public String getNamespace() {
      return worker.getNamespace();
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      workerDao.setNamespace(namespace);
      worker.setNamespace(namespace);
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
