package com.adsoul.redjob.channel.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to stop a currently executing job.
 */
@JsonTypeName
public class StopJob {
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
    * @param id
    *           Id of job to stop.
    */
   public StopJob(long id) {
      this.id = id;
   }

   /**
    * Id of job to stop.
    */
   public long getId() {
      return id;
   }
}
