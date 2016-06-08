package com.s24.redjob.lock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

import com.s24.redjob.AbstractDao;

/**
 * Default implementation of {@link LockDao}.
 */
public class LockDaoImpl extends AbstractDao implements LockDao {
   /**
    * Redis key part for locks.
    */
   public static final String LOCK = "lock";

   /**
    * Redis access.
    */
   private StringRedisTemplate redis;

   @Override
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      redis = new StringRedisTemplate();
      redis.setConnectionFactory(connectionFactory);
      redis.afterPropertiesSet();
   }

   @Override
   public boolean tryLock(final String lock, final String holder, final int timeout, final TimeUnit unit) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");
      Assert.isTrue(timeout > 0, "Pre-condition violated: timeout > 0.");
      Assert.notNull(unit, "Pre-condition violated: unit != null.");

      return redis.execute((RedisConnection connection) -> {
         byte[] key = key(LOCK, lock);
         byte[] value = value(holder);

         return doTryLock(connection, key, value, timeout, unit);
      });
   }

   /**
    * Try to acquire a lock.
    *
    * @param connection
    *           Connection.
    * @param key
    *           Name of the lock.
    * @param value
    *           Holder for the lock.
    * @param timeout
    *           Timeout for the lock. Should be >= 100 ms.
    * @param unit
    *           Unit of the timeout.
    * @return Lock has been acquired.
    */
   private boolean doTryLock(RedisConnection connection, byte[] key, byte[] value, int timeout, TimeUnit unit) {
      Assert.notNull(connection, "Pre-condition violated: connection != null.");
      Assert.notNull(key, "Pre-condition violated: key != null.");
      Assert.notNull(value, "Pre-condition violated: value != null.");
      Assert.isTrue(timeout > 0, "Pre-condition violated: timeout > 0.");
      Assert.notNull(unit, "Pre-condition violated: unit != null.");

      long timeoutMillis = unit.toMillis(timeout);
      Assert.isTrue(timeoutMillis >= 100, "Pre-condition violated: timeoutMillis >= 100.");

      // Try to extend existing lock.
      if (Arrays.equals(value, connection.get(key))) {
         if (connection.expire(key, timeout) && Arrays.equals(value, connection.get(key))) {
            // Expiration has successfully been set and we are the new holder -> We got the lock.
            return true;
         }
      }

      // Try to acquire lock.
      if (!connection.setNX(key, value)) {
         // Key exists and is not set to our holder (see above) -> Lock is hold by someone else.
         return false;
      }
      if (!connection.pExpire(key, timeoutMillis)) {
         // Failed to set expiration -> Maybe someone else deleted our key? -> Lock cannot be acquired now.
         return false;
      }

      // Lock has successfully been acquired if we are finally the new holder.
      return Arrays.equals(value, connection.get(key));
   }

   @Override
   public void releaseLock(final String lock, final String holder) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");

      redis.execute((RedisConnection connection) -> {
         byte[] key = key(LOCK, lock);
         byte[] value = value(holder);

         // Try to acquire lock first to avoid race conditions.
         if (connection.exists(key) && doTryLock(connection, key, value, 1, TimeUnit.SECONDS)) {
            connection.del(key);
         }

         return null;
      });
   }
}
