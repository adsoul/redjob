package com.s24.redjob.worker.events;

import com.s24.redjob.worker.Worker;

/**
 * Worker event.
 */
public interface WorkerEvent {
   /**
    * Worker.
    */
   <W extends Worker> W getWorker();
}
