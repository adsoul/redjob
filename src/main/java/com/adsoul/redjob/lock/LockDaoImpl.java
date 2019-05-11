package com.adsoul.redjob.lock;

import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

import com.adsoul.redjob.AbstractDao;
import com.adsoul.redjob.ByteArrayRedisSerializer;

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
   private RedisTemplate<byte[], byte[]> redis;

   @Override
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(new ByteArrayRedisSerializer());
      redis.setValueSerializer(new ByteArrayRedisSerializer());
      redis.afterPropertiesSet();
   }

   @Override
   public boolean tryLock(final String lock, final String holder, final int timeout, final TimeUnit unit) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");
      Assert.isTrue(timeout > 0, "Pre-condition violated: timeout > 0.");
      Assert.notNull(unit, "Pre-condition violated: unit != null.");
      long timeoutMillis = unit.toMillis(timeout);
      Assert.isTrue(timeoutMillis >= 100, "Pre-condition violated: timeoutMillis >= 100.");

      // KEYS[1]: key
      // ARGV[1]: timeout
      // ARGV[2]: value
      DefaultRedisScript<Boolean> s = new DefaultRedisScript<>(
            "local lock = redis.call('get', KEYS[1]); " +
            "if (lock == ARGV[2]) then " +
               // We own the lock -> Refresh expiration.
               "redis.call('pexpire', KEYS[1], ARGV[1]); " +
               "return true; " +
            "end; " +
            "if (lock) then " +
               // Someone else owns the lock -> Abort.
               "return false; " +
            "end; " +
            // No one owns the lock -> Create lock with expiration.
            "redis.call('psetex', KEYS[1], ARGV[1], ARGV[2]); " +
            "return true;",
            Boolean.class);

      return redis.execute(s, keys(key(LOCK, lock)), value(timeoutMillis), value(holder));
   }

   @Override
   public void releaseLock(final String lock, final String holder) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");

      // KEYS[1]: key
      // ARGV[1]: value
      DefaultRedisScript<Void> s = new DefaultRedisScript<>(
            "local lock = redis.call('get', KEYS[1]); " +
            "if (lock == ARGV[1]) then " +
               // We own the lock -> We may release it by deleting the lock.
               "redis.call('del', KEYS[1]); " +
            "end;");

      redis.execute(s, keys(key(LOCK, lock)), value(holder));
   }

}
