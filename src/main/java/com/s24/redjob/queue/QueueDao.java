package com.s24.redjob.queue;

/**
 * DAO for accessing job queues.
 */
public interface QueueDao {
    /**
     * Enqueue the given job to the given queue.
     *
     * @param queue Queue name.
     * @param payload Payload.
     * @param front Enqueue job at front of the queue, so that the job is the first to be executed?.
     * @return Id assigned to the job.
     */
    long enqueue(String queue, Object payload, boolean front);

    /**
     * Dequeue the job with the given id from the given queue.
     *
     * @param queue Queue name.
     * @param id Id of the job.
     */
    void dequeue(String queue, long id);

    /**
     * Pop first job from queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     * @return Job or null, if none is in the queue.
     */
    Job pop(String queue, String worker);

    /**
     * Remove job from inflight queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     */
    void removeInflight(String queue, String worker);

    /**
     * Restore job from inflight queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     */
    void restoreInflight(String queue, String worker);
}
