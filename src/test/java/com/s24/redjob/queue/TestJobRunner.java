package com.s24.redjob.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

/**
 * Job runner for {@link TestJob}s.
 */
public class TestJobRunner implements Runnable {
   /**
    * If the job's value if {@value #EXCEPTION}, this runner throws an exception when processing it.
    */
   public static final String EXCEPTION = "exception";

   /**
    * Payload
    */
   private static volatile TestJob job;

   /**
    * Latch.
    */
   private static volatile CountDownLatch latch = new CountDownLatch(0);

   /**
    * Constructor.
    *
    * @param job
    *           Job.
    */
   public TestJobRunner(TestJob job) {
      Assert.notNull(job, "Precondition violated: job != null.");

      this.job = job;
   }

   /**
    * Reset latch to the given value.
    */
   public static void resetLatch(int count) {
      latch = new CountDownLatch(count);
   }

   @Override
   public void run() {
      latch.countDown();
      if (EXCEPTION.equals(job.getValue())) {
         throw new Error("exception");
      }
   }

   /**
    * Await latch to be counted down.
    *
    * @return Latch has been counted down to zero.
    */
   public static boolean awaitLatch(long timeout, TimeUnit unit) throws InterruptedException {
      return latch.await(timeout, unit);
   }

   /**
    * Payload.
    */
   public static TestJob getJob() {
      return job;
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof TestJobRunner && job.equals(((TestJobRunner) o).job);
   }

   @Override
   public int hashCode() {
      return job.hashCode();
   }
}
