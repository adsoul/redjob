package com.adsoul.redjob.worker;

/**
 * Worker.
 */
public interface Worker {
   /**
    * Namespace of worker.
    */
   String getNamespace();

   /**
    * Local unique id of worker.
    */
   int getId();

   /**
    * Name of worker.
    */
   String getName();

   /**
    * Start worker.
    */
   void start();

   /**
    * Stop worker.
    */
   void stop();

   /**
    * Wait until worker has stopped.
    */
   void waitUntilStopped();
}
