package com.s24.redjob.worker;

import java.time.LocalDateTime;

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
    * State: Worker failed.
    */
   public static final String FAILED = "failed";

   /**
    * State of worker.
    */
   private String state;

   /**
    * Successful job executions.
    */
   private int success = 0;

   /**
    * Failed job executions.
    */
   private int failed = 0;

   /**
    * Constructor.
    */
   public WorkerState() {
      this.started = LocalDateTime.now();
      this.state = RUNNING;
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
   public void setStarted(LocalDateTime started) {
      this.started = started;
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
   public void setState(String state) {
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
   public void setSuccess(int success) {
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
   public void setFailed(int failed) {
      this.failed = failed;
   }
}
