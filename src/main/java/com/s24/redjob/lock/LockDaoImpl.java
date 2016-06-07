package com.s24.redjob.lock;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import com.s24.redjob.AbstractDao;

/**
 * Default implementation of {@link LockDao}.
 */
public class LockDaoImpl extends AbstractDao {
   /**
    * Redis key part for locks.
    */
   public static final String LOCK = "lock";

   /**
    * Redis access.
    */
   private RedisTemplate<String, ?> redis;

   @Override
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(strings);
      redis.afterPropertiesSet();
   }

   /**
    * Try to acquire a lock.
    *
    * @param lock
    *           Name of the lock.
    * @param holder
    *           Holder for the lock.
    * @param timeout
    *           Timeout in seconds for the lock.
    * @return Lock has been acquired.
    */
   public boolean tryLock(final String lock, final String holder, final int timeout) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");
      Assert.notNull(timeout > 0, "Pre-condition violated: timeout > 0.");

      return redis.execute((RedisConnection connection) -> {
         byte[] key = key(LOCK, lock);
         byte[] value = value(holder);

         // Try to extend existing lock.
         if (value.equals(connection.get(key))) {
            if (connection.expire(key, timeout) && value.equals(connection.get(key))) {
               // Expiration has successfully been set and we are the new holder -> We got the lock.
               return true;
            }
         }

         // Try to acquire lock.


         return false;
      });
   }
}
