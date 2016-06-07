package com.s24.redjob.channel.client;

/**
 * Admin job client.
 */
public interface AdminClient {
   /**
    * Publish the given job to the given channel.
    *
    * @param channel
    *           Channel name.
    * @param job
    *           Job.
    */
   void publish(String channel, Object job);
}
