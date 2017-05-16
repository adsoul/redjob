package com.s24.redjob.client;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.util.Assert;

import com.s24.redjob.channel.ChannelDao;
import com.s24.redjob.lock.LockDao;
import com.s24.redjob.queue.FifoDao;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.WorkerDao;

/**
 * Default implementation of {@link Client}.
 */
public class ClientImpl implements Client {
   /**
    * Worker dao.
    */
   private WorkerDao workerDao;

   /**
    * Queue dao.
    */
   private FifoDao fifoDao;

   /**
    * Channel dao.
    */
   private ChannelDao channelDao;

   /**
    * Lock dao.
    */
   private LockDao lockDao;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(workerDao, "Precondition violated: workerDao != null.");
      Assert.notNull(fifoDao, "Precondition violated: fifoDao != null.");
      Assert.notNull(channelDao, "Precondition violated: channelDao != null.");
      Assert.notNull(lockDao, "Precondition violated: lockDao != null.");
   }

   @Override
   public long enqueue(String queue, Object job, boolean front) {
      return fifoDao.enqueue(queue, job, front).getId();
   }

   @Override
   public void dequeue(String queue, long id) {
      fifoDao.dequeue(queue, id);
   }

   @Override
   public Execution execution(long id) {
      return fifoDao.get(id);
   }

   @Override
   public List<Execution> queuedExecutions(String queue) {
      return fifoDao.getQueued(queue);
   }

   @Override
   public List<Execution> inflightExecutions(String queue) {
      return workerDao.names().stream()
            .map(worker -> fifoDao.getInflight(queue, worker))
            .flatMap(List::stream)
            .collect(toList());
   }

   @Override
   public List<Execution> allExecutions() {
      return fifoDao.getAll();
   }

   @Override
   public long publish(String channel, Object job) {
      return channelDao.publish(channel, job).getId();
   }

   @Override
   public boolean tryLock(String lock, String holder, int timeout, TimeUnit unit) {
      return lockDao.tryLock(lock, holder, timeout, unit);
   }

   @Override
   public void releaseLock(String lock, String holder) {
      lockDao.releaseLock(lock, holder);
   }

   //
   // Injections.
   //

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
    * Queue dao.
    */
   public FifoDao getFifoDao() {
      return fifoDao;
   }

   /**
    * Queue dao.
    */
   public void setFifoDao(FifoDao fifoDao) {
      this.fifoDao = fifoDao;
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
    * Lock dao.
    */
   public LockDao getLockDao() {
      return lockDao;
   }

   /**
    * Lock dao.
    */
   public void setLockDao(LockDao lockDao) {
      this.lockDao = lockDao;
   }
}
