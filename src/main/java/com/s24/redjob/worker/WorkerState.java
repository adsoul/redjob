package com.s24.redjob.worker;

import java.time.LocalDateTime;

/**
 * State of worker.
 */
public class WorkerState {
   /**
    * Start time of worker.
    */
   private LocalDateTime started = LocalDateTime.now();

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
   private String state = RUNNING;

   /**
    * Start time of worker.
    */
   public LocalDateTime getStarted() {
      return started;
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
}
