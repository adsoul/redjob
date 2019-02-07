package com.s24.redjob.channel;

import com.s24.redjob.worker.WorkerState;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Queue worker state.
 */
@JsonTypeName
public class ChannelWorkerState extends WorkerState {
   /**
    * All concrete queues the worker polls.
    */
   private final Set<String> channels = new HashSet<>();

   /**
    * All concrete queues the worker polls.
    */
   public Set<String> getChannels() {
      return channels;
   }

   /**
    * All concrete queues the worker polls.
    */
   public void setChannels(Collection<String> channels) {
      this.channels.clear();
      this.channels.addAll(channels);
   }
}
