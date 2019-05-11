package com.adsoul.redjob;

/**
 * Common interface for all DAOs.
 */
public interface Dao {
   /**
    * Default namespace.
    */
   String DEFAULT_NAMESPACE = "redjob";

   /**
    * Redis "namespace" to use. Prefix for all Redis keys.
    */
   String getNamespace();
}
