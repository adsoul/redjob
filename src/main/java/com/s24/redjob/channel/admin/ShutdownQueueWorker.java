package com.s24.redjob.channel.admin;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to shutdown queue workers.
 */
@JsonTypeName
public class ShutdownQueueWorker {
   /**
    * Constructor to shutdown all workers.
    */
   public ShutdownQueueWorker() {
      super();
   }
}
