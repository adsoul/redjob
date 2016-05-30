package com.s24.redjob.channel.client;

/**
 * Admin job client.
 */
public interface AdminClient {
    /**
     * Publish the given job to the given channel.
     *
     * @param channel Channel name.
     * @param payload Payload.
     */
    void publish(String channel, Object payload);
}
