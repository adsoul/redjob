package com.adsoul.redjob.channel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.adsoul.redjob.TestEventPublisher;
import com.adsoul.redjob.TestRedis;
import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.WorkerDaoImpl;
import com.adsoul.redjob.worker.events.JobExecute;
import com.adsoul.redjob.worker.events.JobProcess;
import com.adsoul.redjob.worker.events.JobStart;
import com.adsoul.redjob.worker.events.JobSuccess;
import com.adsoul.redjob.worker.events.WorkerStart;
import com.adsoul.redjob.worker.events.WorkerStopped;
import com.adsoul.redjob.worker.events.WorkerStopping;
import com.adsoul.redjob.worker.execution.SameThread;
import com.adsoul.redjob.worker.json.TestExecutionRedisSerializer;
import com.adsoul.redjob.worker.runner.TestJob;
import com.adsoul.redjob.worker.runner.TestJobRunner;
import com.adsoul.redjob.worker.runner.TestJobRunnerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@link ChannelDao} and {@link ChannelWorker}.
 */
class ChannelWorkerIT {
   /**
    * Channel DAO.
    */
   private ChannelDao channelDao;

   /**
    * Channel DAO.
    */
   private ChannelWorker channelWorker;

   /**
    * Event publisher.
    */
   private TestEventPublisher eventBus = new TestEventPublisher();

   @BeforeEach
   void setUp() throws Exception {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();

      WorkerDaoImpl workerDao = new WorkerDaoImpl();
      workerDao.setConnectionFactory(connectionFactory);
      workerDao.setNamespace("test");
      workerDao.afterPropertiesSet();

      RedisMessageListenerContainer listenerContainer = new RedisMessageListenerContainer();
      listenerContainer.setConnectionFactory(connectionFactory);
      listenerContainer.afterPropertiesSet();
      listenerContainer.start();

      ChannelDaoImpl channelDao = new ChannelDaoImpl();
      channelDao.setConnectionFactory(connectionFactory);
      channelDao.setNamespace("test");
      channelDao.setExecutions(new TestExecutionRedisSerializer(TestJob.class));
      channelDao.afterPropertiesSet();

      ChannelWorkerFactoryBean factory = new ChannelWorkerFactoryBean();
      factory.setWorkerDao(workerDao);
      factory.setChannelDao(channelDao);
      factory.setChannels("test-channel");
      factory.setListenerContainer(listenerContainer);
      factory.setApplicationEventPublisher(eventBus);
      factory.setExecutionStrategy(new SameThread(new TestJobRunnerFactory()));
      factory.afterPropertiesSet();

      channelWorker = factory.getObject();
      this.channelDao = channelWorker.getChannelDao();
      channelWorker.start();

      // Wait for subscription of worker to the channel.
      Thread.sleep(1000);
   }

   @AfterEach
   void tearDown() {
      eventBus.doNotBlock();
      channelWorker.stop();
   }

   @Test
   void testLifecycle() throws Exception {
      TestJob job = new TestJob();
      TestJobRunner runner = new TestJobRunner(job);

      assertEquals(new WorkerStart(channelWorker), eventBus.waitForEvent());

      Execution execution = channelDao.publish("test-channel", job);

      assertEquals(new JobProcess(channelWorker, "test-channel", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(channelWorker, "test-channel", execution), eventBus.waitForEvent());
      assertEquals(new JobStart(channelWorker, "test-channel", execution), eventBus.waitForEvent());

      // Asynchronously stop worker, because stop blocks until the last job finished.
      new Thread(channelWorker::stop).start();

      assertEquals(new WorkerStopping(channelWorker), eventBus.waitForEvent());
      assertEquals(new JobSuccess(channelWorker, "test-channel", execution), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());

      // Worker stop should always be published, if the last worker has finished.
      assertEquals(new WorkerStopped(channelWorker), eventBus.waitForEvent());
   }
}
