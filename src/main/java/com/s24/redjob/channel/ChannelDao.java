package com.s24.redjob.channel;

import org.springframework.data.redis.connection.Message;

import com.s24.redjob.worker.Execution;

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
   Execution publish(String channel, Object job);

   /**
    * Extract channel from channel message.
    *
    * @param message
    *           Message.
    */
   String getChannel(Message message);

   /**
    * Extract execution from channel message.
    *
    * @param message
    *           Message.
    */
   Execution getExecution(Message message);
}
