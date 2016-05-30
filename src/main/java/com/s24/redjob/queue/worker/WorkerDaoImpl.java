package com.s24.redjob.queue.worker;

import com.s24.redjob.AbstractDao;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Default implementation of {@link WorkerDao}.
 */
public class WorkerDaoImpl extends AbstractDao implements WorkerDao {
    /**
     * Redis key part for set of all worker names.
     */
    public static final String WORKERS = "workers";

    /**
     * Redis key part for worker.
     */
    public static final String WORKER = "worker";

    /**
     * Redis key part for worker start time.
     */
    public static final String STARTED = "started";

    /**
     * Redis key part for worker stats.
     */
    public static final String STAT = "stat";

    /**
     * Redis key part for number of processed jobs.
     */
    public static final String PROCESSED = "processed";

    /**
     * Redis key part for number of failed jobs.
     */
    public static final String FAILED = "failed";

    /**
     * Redis access.
     */
    private RedisTemplate<String, String> redis;

    @Override
    @PostConstruct
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        redis = new RedisTemplate<>();
        redis.setConnectionFactory(connectionFactory);
        redis.setKeySerializer(strings);
        redis.setValueSerializer(strings);
        redis.afterPropertiesSet();
    }

    @Override
    public void start(String name) {
        Assert.notNull(name, "Precondition violated: name != null.");

        redis.execute((RedisConnection connection) -> {
            connection.sAdd(key(WORKERS), value(name));
            connection.set(key(WORKER, name, STARTED), value(LocalDateTime.now()));
            return null;
        });
    }

    @Override
    public void stop(String name) {
        Assert.notNull(name, "Precondition violated: name != null.");

        redis.execute((RedisConnection connection) -> {
            connection.sRem(key(WORKERS), value(name));
            connection.del(key(WORKER, name, STARTED), key(WORKER, name),
                    key(STAT, PROCESSED, name), key(STAT, FAILED, name));
            return null;
        });
    }

    @Override
    public void success(String name) {
        Assert.notNull(name, "Precondition violated: name != null.");

        redis.execute((RedisConnection connection) -> {
            connection.incr(key(STAT, PROCESSED));
            connection.incr(key(STAT, PROCESSED, name));
            return null;
        });
    }

    @Override
    public void failure(String name) {
        Assert.notNull(name, "Precondition violated: name != null.");

        redis.execute((RedisConnection connection) -> {
            connection.incr(key(STAT, FAILED));
            connection.incr(key(STAT, FAILED, name));
            return null;
        });
    }

    //
    // Serialization.
    //

    /**
     * Serialize timestamp.
     *
     * @param value Timestamp.
     * @return Serialized timestamp.
     */
    protected byte[] value(LocalDateTime value) {
        return value(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
