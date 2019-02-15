package com.s24.redjob.channel.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to stop a currently executing job.
 */
@JsonTypeName
public class StopJob {
   /**
    * Namespace.
    */
   private String namespace;

   /**
    * Id of job to stop.
    */
   private long id;

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   StopJob() {
      super();
   }

   /**
    * Constructor to stop the given job.
    *
    * @param namespace
    *           Namespace.
    * @param id
    *           Id of job to stop.
    */
   public StopJob(String namespace, long id) {
      this.namespace = namespace;
      this.id = id;
   }

   /**
    * Namespace.
    */
   public String getNamespace() {
      return namespace;
   }

   /**
    * Id of job to stop.
    */
   public long getId() {
      return id;
   }
}
