package com.adsoul.redjob.lock;

import java.util.concurrent.TimeUnit;

/**
 * DAO for distributed locks.
 */
public interface LockDao {
   /**
    * Try to acquire a lock.
    *
    * @param lock
    *           Name of the lock.
    * @param holder
    *           Holder for the lock.
    * @param timeout
    *           Timeout for the lock. Should be >= 100 ms.
    * @param unit
    *           Unit of the timeout.
    * @return Lock has been acquired.
    */
   boolean tryLock(String lock, String holder, int timeout, TimeUnit unit);

   /**
    * Release a lock.
    *
    * This operations does nothing, if the lock is not held by the given holder.
    *
    * Nevertheless it should not be called unnecessarily,
    * because it acquires the lock for one second to avoid race conditions.
    *
    * @param lock
    *           Name of the lock.
    * @param holder
    *           Holder for the lock.
    */
   void releaseLock(String lock, String holder);
}
