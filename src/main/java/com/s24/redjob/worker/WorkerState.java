package com.s24.redjob.worker;

import java.time.LocalDateTime;

import static java.util.Arrays.stream;

/**
 * State of worker.
 */
public class WorkerState {
   /**
    * Start time of worker.
    */
   private LocalDateTime started;

   /**
    * State: Worker runs.
    */
   public static final String INIT = "init";

   /**
    * State: Worker runs.
    */
   public static final String RUNNING = "running";

   /**
    * State: Worker pause.
    */
   public static final String PAUSED = "paused";

   /**
    * State: Worker is stopping.
    */
   public static final String STOPPING = "stopping";

   /**
    * State: Worker has stopped.
    */
   public static final String STOPPED = "stopped";

   /**
    * State: Worker failed.
    */
   public static final String FAILED = "failed";

   /**
    * State of worker.
    */
   private volatile String state = INIT;

   /**
    * Successful job executions.
    */
   private int success = 0;

   /**
    * Failed job executions.
    */
   private int failed = 0;

   /**
    * Start worker.
    */
   public void start() {
      this.started = LocalDateTime.now();
      this.state = RUNNING;
   }

   /**
    * Pause worker.
    */
   public void pause() {
      this.state = PAUSED;
   }

   /**
    * Stopping worker.
    */
   public void stop() {
      this.state = STOPPING;
   }

   /**
    * Is the worker stopping?.
    */
   public boolean isStopping() {
      return isState(STOPPING);
   }

   /**
    * Worker stopped.
    */
   public void stopped() {
      this.state = STOPPED;
   }

   /**
    * Worker failed.
    */
   public void failed() {
      this.state = FAILED;
   }

   /**
    * Has the worker failed?.
    */
   public boolean isFailed() {
      return isState(FAILED);
   }

   /**
    * Has the worker terminated?.
    */
   public boolean isTerminated() {
      return isState(STOPPED, FAILED);
   }

   /**
    * Start time of worker.
    */
   public LocalDateTime getStarted() {
      return started;
   }

   /**
    * Start time of worker.
    */
   void setStarted(LocalDateTime started) {
      this.started = started;
   }

   /**
    * Is the state one of the given ones?.
    *
    * @params states States.
    */
   boolean isState(String... states) {
      return stream(states).anyMatch(state::equals);
   }

   /**
    * State of worker.
    */
   public String getState() {
      return state;
   }

   /**
    * Set state of worker.
    */
   void setState(String state) {
      this.state = state;
   }

   /**
    * Increase number of successful job executions.
    */
   public void incSuccess() {
      success++;
   }

   /**
    * Successful job executions.
    */
   public int getSuccess() {
      return success;
   }

   /**
    * Successful job executions.
    */
   void setSuccess(int success) {
      this.success = success;
   }

   /**
    * Increase number of failed job executions.
    */
   public void incFailed() {
      failed++;
   }

   /**
    * Failed job executions.
    */
   public int getFailed() {
      return failed;
   }

   /**
    * Failed job executions.
    */
   void setFailed(int failed) {
      this.failed = failed;
   }
}
