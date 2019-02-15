package com.s24.redjob.channel.command;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import static java.util.Collections.emptyList;

/**
 * Command to shutdown queue workers.
 */
@JsonTypeName
public class PauseQueueWorker {
   /**
    * Pause (true) or unpause (false) workers?.
    */
   private boolean pause;

   /**
    * Namespace.
    */
   private String namespace;

   /**
    * Workers of which queues should be paused.
    * If empty, all workers will be paused.
    */
   private final Set<String> queues = new HashSet<>();

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   PauseQueueWorker() {
   }

   /**
    * Constructor to pause/unpause all workers.
    *
    * @param namespace
    *           Namespace.
    */
   public PauseQueueWorker(String namespace, boolean pause) {
      this(namespace, pause, emptyList());
   }

   /**
    * Constructor to pause/unpause workers processing the given queues.
    *
    * @param namespace
    *           Namespace.
    * @param pause
    *           Pause (true) or unpause (false)?
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public PauseQueueWorker(String namespace, boolean pause, String... queues) {
      this(namespace, pause, Arrays.asList(queues));
   }

   /**
    * Constructor to pause/unpause workers processing the given queues.
    *
    * @param namespace
    *           Namespace.
    * @param pause
    *           Pause (true) or unpause (false)?
    * @param queues
    *           Queues to select workers. If empty, select all workers.
    */
   public PauseQueueWorker(String namespace, boolean pause, Collection<String> queues) {
      this.namespace = namespace;
      this.pause = pause;
      this.queues.addAll(queues);
   }

   /**
    * Pause (true) or unpause (false) workers?.
    */
   public boolean isPause() {
      return pause;
   }

   /**
    * Namespace.
    */
   public String getNamespace() {
      return namespace;
   }

   /**
    * Workers of which queues should be paused.
    * If empty, all workers will be paused.
    */
   public Set<String> getQueues() {
      return queues;
   }
}
