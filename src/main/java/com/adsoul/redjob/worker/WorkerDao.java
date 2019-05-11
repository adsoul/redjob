package com.adsoul.redjob.worker;

import java.util.Set;

import com.adsoul.redjob.Dao;

/**
 * DAO for worker stats.
 */
public interface WorkerDao extends Dao {
   /**
    * Ping Redis.
    */
   void ping();

   /**
    * Update worker state.
    *
    * @param name
    *           Name of worker.
    * @param state
    *           State of worker.
    */
   void state(String name, WorkerState state);

   /**
    * Stop of worker.
    *
    * @param name
    *           Name of worker.
    */
   void stop(String name);

   /**
    * Job has been successfully been processed.
    *
    * @param name
    *           Name of worker.
    */
   void success(String name);

   /**
    * Job execution failed.
    *
    * @param name
    *           Name of worker.
    */
   void failure(String name);

   /**
    * Names of all active workers.
    */
   Set<String> names();
}
