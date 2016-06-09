package com.s24.redjob.client;

import java.util.concurrent.TimeUnit;

/**
 * Job client.
 */
public interface Client {
   /**
    * Enqueue the given job to the given queue.
    *
    * @param queue
    *           Queue name.
    * @param job
    *           Job.
    * @return Id assigned to the job.
    */
   default long enqueue(String queue, Object job) {
      return enqueue(queue, job, false);
   }

   /**
    * Enqueue the given job to the given queue.
    *
    * @param queue
    *           Queue name.
    * @param job
    *           Job.
    * @param front
    *           Enqueue job at front of the queue, so that the job is the first to be executed?.
    * @return Id assigned to the job.
    */
   long enqueue(String queue, Object job, boolean front);

   /**
    * Dequeue the job with the given id from the given queue.
    *
    * @param queue
    *           Queue name.
    * @param id
    *           Id of the job.
    */
   void dequeue(String queue, long id);

   /**
    * Publish the given job to the given channel.
    *
    * @param channel
    *           Channel name.
    * @param job
    *           Job.
    * @return Id assigned to the job.
    */
   long publish(String channel, Object job);

   /**
    * Try to acquire a lock.
    *
    * @param lock
    *           Name of the lock.
    * @param holder
    *           Holder for the lock.
    * @param timeout
    *           Timeout for the lock. Should be >= 100 ms.
    * @param unit
    *           Unit of the timeout.
    * @return Lock has been acquired.
    */
   boolean tryLock(String lock, String holder, int timeout, TimeUnit unit);

   /**
    * Release a lock.
    *
    * @param lock
    *           Name of the lock.
    * @param holder
    *           Holder for the lock.
    */
   void releaseLock(String lock, String holder);
}
