package com.s24.redjob.worker;

import org.springframework.util.Assert;

/**
 * Wrapper for job runners which do not implement {@link Runnable}.
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
      Assert.notNull(runner, "Pre-condition violated: runner != null.");
      this.runner = runner;
   }

   /**
    * Job runner.
    */
   @SuppressWarnings("unchecked")
   public <R> R unwrap() {
      return (R) runner;
   }
}
