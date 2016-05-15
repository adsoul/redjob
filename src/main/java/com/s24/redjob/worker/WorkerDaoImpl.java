package com.s24.redjob.worker;

import com.s24.redjob.AbstractDao;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkNotNull;

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
    public void start(Worker worker) {
        checkNotNull(worker, "Precondition violated: worker != null");

        redis.execute((RedisConnection connection) -> {
            connection.sAdd(key(WORKERS), value(worker.getName()));
            connection.set(key(WORKER, worker.getName(), STARTED), value(LocalDateTime.now()));
            return null;
        });
    }

    @Override
    public void stop(Worker worker) {
        checkNotNull(worker, "Precondition violated: worker != null");

        redis.execute((RedisConnection connection) -> {
            connection.sRem(key(WORKERS), value(worker.getName()));
            connection.del(key(WORKER, worker.getName(), STARTED), key(WORKER, worker.getName()),
                    key(STAT, PROCESSED, worker.getName()), key(STAT, FAILED, worker.getName()));
            return null;
        });
    }

    @Override
    public void success(Worker worker) {
        checkNotNull(worker, "Precondition violated: worker != null");

        redis.execute((RedisConnection connection) -> {
            connection.incr(key(STAT, PROCESSED));
            connection.incr(key(STAT, PROCESSED, worker.getName()));
            return null;
        });
    }

    @Override
    public void failure(Worker worker) {
        checkNotNull(worker, "Precondition violated: worker != null");

        redis.execute((RedisConnection connection) -> {
            connection.incr(key(STAT, FAILED));
            connection.incr(key(STAT, FAILED, worker.getName()));
            return null;
        });
    }

    //
    // Serialization.
    //

    /**
     * Serialize long value.
     *
     * @param value Long.
     * @return Serialized long.
     */
    protected byte[] value(LocalDateTime value) {
        return value(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
