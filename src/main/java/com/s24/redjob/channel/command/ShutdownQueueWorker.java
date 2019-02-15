package com.s24.redjob.channel.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to shutdown queue workers.
 */
@JsonTypeName
public class ShutdownQueueWorker {
   /**
    * Namespace.
    */
   private String namespace;

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   ShutdownQueueWorker() {
   }

   /**
    * Constructor to shutdown all workers.
    *
    * @param namespace
    *           Namespace.
    */
   public ShutdownQueueWorker(String namespace) {
      this.namespace = namespace;
   }

   /**
    * Namespace.
    */
   public String getNamespace() {
      return namespace;
   }
}
