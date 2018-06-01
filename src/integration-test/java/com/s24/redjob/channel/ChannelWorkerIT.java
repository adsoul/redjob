package com.s24.redjob.channel;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.WorkerDaoImpl;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobStart;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import com.s24.redjob.worker.events.WorkerStopping;
import com.s24.redjob.worker.json.TestExecutionRedisSerializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

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
      factory.setJobRunnerFactory(new TestJobRunnerFactory());
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
      assertEquals(new JobExecute(channelWorker, "test-channel", execution, runner), eventBus.waitForEvent());
      assertEquals(new JobStart(channelWorker, "test-channel", execution, runner), eventBus.waitForEvent());

      // Asynchronously stop worker, because stop blocks until the last job finished.
      new Thread(channelWorker::stop).start();

      assertEquals(new WorkerStopping(channelWorker), eventBus.waitForEvent());
      assertEquals(new JobSuccess(channelWorker, "test-channel", execution, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());

      // Worker stop should always be published, if the last worker has finished.
      assertEquals(new WorkerStopped(channelWorker), eventBus.waitForEvent());
   }
}
