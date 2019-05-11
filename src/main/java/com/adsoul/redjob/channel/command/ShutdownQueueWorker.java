package com.adsoul.redjob.channel.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to shutdown queue workers.
 */
@JsonTypeName
public class ShutdownQueueWorker {
   /**
    * Constructor to shutdown all workers.
    */
   @JsonCreator
   public ShutdownQueueWorker() { }
}
