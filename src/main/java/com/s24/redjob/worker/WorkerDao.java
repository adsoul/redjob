package com.s24.redjob.worker;

import com.s24.redjob.Dao;

/**
 * DAO for worker stats.
 */
public interface WorkerDao extends Dao {
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
}
