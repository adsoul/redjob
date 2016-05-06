package com.s24.resque.worker;

/**
 * Interface for factories creating job runners.
 */
public interface JobRunnerFactory {
    /**
     * Get job runner for payload.
     *
     * @param payload Payload.
     * @return Job runner.
     */
    Runnable runnerFor(Object payload);
}
