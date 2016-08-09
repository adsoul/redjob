package com.s24.redjob.channel;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import com.s24.redjob.worker.json.ExecutionRedisSerializer;
import com.s24.redjob.worker.json.TestExecutionRedisSerializer;

/**
 * Integration test for {@link ChannelDao} and {@link ChannelWorker}.
 */
public class ChannelWorkerIT {
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

   @Before
   public void setUp() throws Exception {
      RedisConnectionFactory connectionFactory = TestRedis.connectionFactory();

      RedisMessageListenerContainer listenerContainer = new RedisMessageListenerContainer();
      listenerContainer.setConnectionFactory(connectionFactory);
      listenerContainer.afterPropertiesSet();
      listenerContainer.start();

      ExecutionRedisSerializer executions = new TestExecutionRedisSerializer(TestJob.class);

      ChannelWorkerFactoryBean factory = new ChannelWorkerFactoryBean();
      factory.setConnectionFactory(connectionFactory);
      factory.setNamespace("test");
      factory.setChannels("test-channel");
      factory.setExecutions(executions);
      factory.setListenerContainer(listenerContainer);
      factory.setApplicationEventPublisher(eventBus);
      factory.setJobRunnerFactory(new TestJobRunnerFactory());
      factory.afterPropertiesSet();

      channelWorker = factory.getObject();
      channelDao = channelWorker.getChannelDao();
      channelWorker.start();

      // Wait for subscription of worker to the channel.
      Thread.sleep(1000);
   }

   @After
   public void tearDown() throws Exception {
      eventBus.doNotBlock();
      channelWorker.stop();
   }

   @Test
   public void testLifecycle() throws Exception {
      TestJob job = new TestJob();
      TestJobRunner runner = new TestJobRunner(job);

      assertEquals(new WorkerStart(channelWorker), eventBus.waitForEvent());

      Execution execution = channelDao.publish("test-channel", job);

      assertEquals(new JobProcess(channelWorker, "test-channel", execution), eventBus.waitForEvent());
      assertEquals(new JobExecute(channelWorker, "test-channel", execution, runner), eventBus.waitForEvent());

      // Asynchronously stop worker, because stop blocks until the last job finished.
      new Thread(channelWorker::stop).start();

      assertEquals(new JobSuccess(channelWorker, "test-channel", execution, runner), eventBus.waitForEvent());
      assertEquals(job, TestJobRunner.getLastJob());

      // Worker stop should always be published, if the last worker has finished.
      assertEquals(new WorkerStopped(channelWorker), eventBus.waitForEvent());
   }
}
