package com.s24.redjob.channel.command;

import com.s24.redjob.queue.QueueWorker;

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

   /**
    * Does the worker match the selectors of the job?.
    */
   boolean matches(QueueWorker worker) {
      return worker.getNamespace().equals(namespace);
   }
}
