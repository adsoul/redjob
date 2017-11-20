package com.s24.redjob.lock;

import com.s24.redjob.AbstractDao;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

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

      DefaultRedisScript<Boolean> s = new DefaultRedisScript<>(
            "local key = KEYS[1]; " +
            "local value = ARGV[1]; " +
            "local timeout = ARGV[2]; " +

            "local lock = redis.call('get', key); " +
            "if (lock == value) then " +
               "redis.call('pexpire', key, timeout); " +
               "return true; " +
            "end; " +
            "if (lock) then " +
               "return false; " +
            "end; " +
            "redis.call('psetex', key, timeout, value); " +
            "return true;",
            Boolean.class);

      long timeoutMillis = unit.toMillis(timeout);
      Assert.isTrue(timeoutMillis >= 100, "Pre-condition violated: timeoutMillis >= 100.");

      return redis.execute(s, singletonList(keyString(LOCK, lock)), holder, Long.toString(timeoutMillis));
   }

   @Override
   public void releaseLock(final String lock, final String holder) {
      Assert.notNull(lock, "Pre-condition violated: lock != null.");
      Assert.notNull(holder, "Pre-condition violated: holder != null.");

      DefaultRedisScript<Void> s = new DefaultRedisScript<>(
            "local key = KEYS[1]; " +
            "local value = ARGV[1]; " +

            "local lock = redis.call('get', key); " +
            "if (lock == value) then " +
               "redis.call('del', key); " +
            "end;");

      redis.execute(s, singletonList(keyString(LOCK, lock)), holder);
   }
}
