package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Execution;

/**
 * Worker job event.
 */
public interface JobEvent extends WorkerEvent, QueueEvent {
   /**
    * Job execution.
    */
   Execution getExecution();
}
