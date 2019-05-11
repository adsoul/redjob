package com.adsoul.redjob.worker.events;

import com.adsoul.redjob.worker.Execution;

/**
 * Worker job event.
 */
public interface JobEvent extends WorkerEvent, QueueEvent {
   /**
    * Job execution.
    */
   Execution getExecution();
}
