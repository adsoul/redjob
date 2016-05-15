package com.s24.redjob.worker;

/**
 * DAO for worker stats.
 */
public interface WorkerDao {
    /**
     * Start of worker.
     */
    void start(Worker worker);

    /**
     * Stop of worker.
     */
    void stop(Worker worker);

    /**
     * Job has been successfully been processed.
     */
    void success(Worker worker);

    /**
     * Job execution failed.
     */
    void failure(Worker worker);
}
