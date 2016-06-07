package com.s24.redjob.channel;

/**
 * DAO for accessing admin job channels.
 */
public interface ChannelDao {
   /**
    * Publish the given admin job to the given channel.
    *
    * @param channel
    *           Channel name.
    * @param job
    *           Job.
    */
   void publish(String channel, Object job);
}
