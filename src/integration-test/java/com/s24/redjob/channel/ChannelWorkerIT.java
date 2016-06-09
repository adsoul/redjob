package com.s24.redjob.channel;

import com.s24.redjob.TestEventPublisher;
import com.s24.redjob.TestRedis;
import com.s24.redjob.queue.JobTypeScannerTest;
import com.s24.redjob.queue.TestJob;
import com.s24.redjob.queue.TestJobRunner;
import com.s24.redjob.queue.TestJobRunnerFactory;
import com.s24.redjob.worker.ExecutionRedisSerializer;
import com.s24.redjob.worker.WorkerDaoImpl;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.concurrent.TimeUnit;

import static com.s24.redjob.queue.JobTypeScannerTest.scanForJsonSubtypes;
import static org.junit.Assert.assertEquals;

/**
 * Integration test for {@link ChannelDao} and {@link ChannelWorker}.
 */
public class ChannelWorkerIT {
   /**
    * Channel DAO.
    */
   private ChannelDaoImpl channelDao;

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
      TestRedis.flushDb(connectionFactory);

      RedisMessageListenerContainer listenerContainer = new RedisMessageListenerContainer();
      listenerContainer.setConnectionFactory(connectionFactory);
      listenerContainer.afterPropertiesSet();
      listenerContainer.start();

      ExecutionRedisSerializer executions = new ExecutionRedisSerializer();
      scanForJsonSubtypes(executions, TestJob.class);

      channelDao = new ChannelDaoImpl();
      channelDao.setConnectionFactory(connectionFactory);
      channelDao.setExecutions(executions);
      channelDao.setNamespace("test");
      channelDao.afterPropertiesSet();

      WorkerDaoImpl workerDao = new WorkerDaoImpl();
      workerDao.setConnectionFactory(connectionFactory);
      workerDao.setNamespace("test");
      workerDao.afterPropertiesSet();

      channelWorker = new ChannelWorker();
      channelWorker.setWorkerDao(workerDao);
      channelWorker.setChannelDao(channelDao);
      channelWorker.setChannels("test-channel");
      channelWorker.setListenerContainer(listenerContainer);
      channelWorker.setApplicationEventPublisher(eventBus);
      channelWorker.setJobRunnerFactory(new TestJobRunnerFactory());
      channelWorker.afterPropertiesSet();

      // Wait for subscription of worker to the channel.
      Thread.sleep(1000);
   }

   @After
   public void tearDown() throws Exception {
      channelWorker.stop();
   }

   @Test
   public void test() throws Exception {
      TestJob job = new TestJob("ping");
      TestJobRunner runner = new TestJobRunner(job);

      assertEquals(new WorkerStart(channelWorker), eventBus.waitForEvent());

      channelDao.publish("test-channel", job);

      assertEquals(new JobProcess(channelWorker, "test-channel", job), eventBus.waitForEvent());
      assertEquals(new JobExecute(channelWorker, "test-channel", job, runner), eventBus.waitForEvent());
      TestJobRunner.awaitLatch(1, TimeUnit.SECONDS);
      assertEquals(new JobSuccess(channelWorker, "test-channel", job, runner), eventBus.waitForEvent());

      channelWorker.stop();

      assertEquals(job, TestJobRunner.getJob());
      assertEquals(new WorkerStopped(channelWorker), eventBus.waitForEvent());
   }
}
