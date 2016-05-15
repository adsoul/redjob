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
}
