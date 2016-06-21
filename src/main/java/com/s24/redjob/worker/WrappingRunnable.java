package com.s24.redjob.worker;

/**
 * Wrapper for job runner which do not implement {@link Runnable}.
 */
public abstract class WrappingRunnable implements Runnable {
   /**
    * Job runner.
    */
   private final Object runner;

   /**
    * Constructor.
    * 
    * @param runner
    *           Job runner.
    */
   public WrappingRunnable(Object runner) {
      this.runner = runner;
   }

   /**
    * Job runner.
    */
   public Object unwrap() {
      return runner;
   }
}
