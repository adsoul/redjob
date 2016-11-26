package com.s24.redjob.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Command to clean up jobs. This job deletes all not deserializable job executions.
 */
@JsonTypeName
public class CleanUpJobs {
   /**
    * Namespaces which should be cleaned up.
    * If empty, all namespaces will be cleaned up.
    */
   private Set<String> namespaces = new HashSet<>();

   /**
    * Constructor to clean up the given namespaces.
    *
    * @param namespaces
    *           Namespaces to clean up. If empty, select all workers.
    */
   public CleanUpJobs(String... namespaces) {
      this(Arrays.asList(namespaces));
   }

   /**
    * Constructor to clean up the given namespaces.
    *
    * @param namespaces
    *           Namespaces to clean up. If empty, select all workers.
    */
   public CleanUpJobs(Collection<String> namespaces) {
      this.namespaces.addAll(namespaces);
   }

   /**
    * Hidden default constructor for Jackson.
    */
   @JsonCreator
   CleanUpJobs() {
   }

   /**
    * Namespaces which should be cleaned up.
    * If empty, all namespaces will be cleaned up.
    */
   public Set<String> getNamespaces() {
      return namespaces;
   }

   /**
    * Does the worker match the selectors of the job?.
    */
   protected boolean matches(FifoDao fifoDao) {
      return namespaces.isEmpty() || namespaces.contains(fifoDao.getNamespace());
   }
}
