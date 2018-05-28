package com.s24.redjob.worker;

import com.s24.redjob.channel.ChannelWorker;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;

/**
 * {@link FactoryBean} for easy creation of a {@link ChannelWorker}.
 */
public abstract class AbstractWorkerFactoryBean<W extends AbstractWorker>
      implements SmartFactoryBean<W>, InitializingBean, DisposableBean, SmartLifecycle, ApplicationEventPublisherAware {
   /**
    * Worker dao.
    */
   private WorkerDao workerDao;

   /**
    * The instance.
    */
   protected final W worker;

   /**
    * Is the worker running?.
    */
   private volatile boolean run = false;

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
      worker.setWorkerDao(workerDao);
      worker.afterPropertiesSet();
   }

   @Override
   public void destroy() throws Exception {
      worker.destroy();
   }

   //
   // Factory bean
   //

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
   @SuppressWarnings("unchecked")
   public Class<W> getObjectType() {
      return (Class<W>) worker.getClass();
   }

   @Override
   public W getObject() throws Exception {
      return worker;
   }

   //
   // Lifecycle
   //

   @Override
   public int getPhase() {
      return Integer.MAX_VALUE;
   }

   @Override
   public boolean isAutoStartup() {
      return true;
   }

   @Override
   public boolean isRunning() {
      return run;
   }

   @Override
   public void start() {
      if (run) {
         return;
      }

      try {
         worker.start();
      } finally {
         run = true;
      }
   }

   @Override
   public void stop(Runnable callback) {
      new Thread(() -> {
         try {
            stop();
         } finally {
            run = false;
         }
      }, "Stopping " + worker.getName()).start();
   }

   @Override
   public void stop() {
      if (!run) {
         return;
      }

      try {
         worker.stop();
      } finally {
         run = false;
      }
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
    * Worker dao.
    */
   public WorkerDao getWorkerDao() {
      return workerDao;
   }

   /**
    * Worker dao.
    */
   public void setWorkerDao(WorkerDao workerDao) {
      this.workerDao = workerDao;
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
