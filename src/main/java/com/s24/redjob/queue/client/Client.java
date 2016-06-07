package com.s24.redjob.queue.client;

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
}
