package com.s24.redjob.channel.admin;

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
    * Default constructor for Jackson.
    */
   private StopJob() {
      super();
   }

   /**
    * Constructor to stop the given job.
    *
    * @param id
    *           Id of job to stop.
    */
   public StopJob(long id) {
      super();
      this.id = id;
   }

   /**
    * Id of job to stop.
    */
   public long getId() {
      return id;
   }
}
