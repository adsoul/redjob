package com.s24.redjob.queue.worker;

/**
 * Worker.
 */
public interface Worker {
   /**
    * Local unique id of worker.
    */
   int getId();

   /**
    * Name of worker.
    */
   String getName();

   /**
    * Stop worker.
    */
   void stop();
}
