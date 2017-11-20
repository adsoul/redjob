package com.s24.redjob.lock;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.ByteArrayRedisSerializer;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

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

      return redis.execute(s, keys(key(LOCK, lock)), value(holder), value(timeoutMillis));
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

      redis.execute(s, keys(key(LOCK, lock)), value(holder));
   }

}
