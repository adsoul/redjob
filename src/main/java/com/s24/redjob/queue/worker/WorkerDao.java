package com.s24.redjob.queue.worker;

/**
 * DAO for worker stats.
 */
public interface WorkerDao {
    /**
     * Start of worker.
     *
     * @param name Name of worker.
     */
    void start(String name);

    /**
     * Stop of worker.
     *
     * @param name Name of worker.
     */
    void stop(String name);

    /**
     * Job has been successfully been processed.
     *
     * @param name Name of worker.
     */
    void success(String name);

    /**
     * Job execution failed.
     *
     * @param name Name of worker.
     */
    void failure(String name);
}
