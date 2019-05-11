package com.adsoul.redjob.worker.events;

import com.adsoul.redjob.worker.Worker;

/**
 * Worker event.
 */
public interface WorkerEvent {
   /**
    * Worker.
    */
   <W extends Worker> W getWorker();
}
