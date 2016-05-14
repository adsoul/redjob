package com.s24.redjob.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Dao for accessing job queues.
 */
public class QueueDaoImpl {
    /**
     * Redis key part for id sequence.
     */
    public static final String ID = "id";

    /**
     * Redis key part for set of all queue names.
     */
    public static final String QUEUES = "queues";

    /**
     * Redis key part for the list of all job ids of a queue.
     */
    public static final String QUEUE = "queue";

    /**
     * Redis key part for the hash of id -> job.
     */
    public static final String JOB = "job";

    /**
     * Redis key part for the list of all job ids of a queue.
     */
    public static final String INFLIGHT = "inflight";

    /**
     * {@link RedisConnectionFactory} to access Redis.
     */
    private RedisConnectionFactory connectionFactory;

    /**
     * Redis serializer for strings.
     */
    private final StringRedisSerializer strings = new StringRedisSerializer();

    /**
     * Redis serializer for jobs.
     */
    private final Jackson2JsonRedisSerializer<Job> jobs = new Jackson2JsonRedisSerializer<>(Job.class);

    /**
     * Redis access.
     */
    private RedisTemplate<String, String> redis;

    /**
     * JSON mapper.
     */
    private ObjectMapper json = new ObjectMapper();

    /**
     * Default namespace.
     */
    public static final String DEFAULT_NAMESPACE = "redjob";

    /**
     * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
     */
    private String namespace = DEFAULT_NAMESPACE;

    /**
     * Init.
     */
    @PostConstruct
    public void afterPropertiesSet() {
        Assert.notNull(connectionFactory, "Precondition violated: connectionFactory != null.");
        Assert.hasLength(namespace, "Precondition violated: namespace has length.");

        jobs.setObjectMapper(json);

        redis = new RedisTemplate<>();
        redis.setConnectionFactory(connectionFactory);
        redis.setKeySerializer(strings);
        redis.setValueSerializer(strings);
        redis.setHashKeySerializer(strings);
        redis.setHashValueSerializer(jobs);
        redis.afterPropertiesSet();
    }

    //
    // Client related.
    //

    /**
     * Enqueue the given job to the given queue.
     *
     * @param queue Queue name.
     * @param payload Payload.
     * @param front Enqueue job at front of the queue, so that the job is the first to be executed?.
     * @return Id assigned to the job.
     */
    public long enqueue(String queue, Object payload, boolean front) {
        return redis.execute((RedisConnection connection) -> {
            Long id = connection.incr(key(ID));
            Job job = new Job(id, payload);

            connection.sAdd(key(QUEUES), value(queue));
            byte[] idBytes = value(id);
            connection.hSet(key(JOB, queue), idBytes, jobs.serialize(job));
            if (front) {
                connection.lPush(key(QUEUE, queue), idBytes);
            } else {
                connection.rPush(key(QUEUE, queue), idBytes);
            }

            return id;
        });
    }

    /**
     * Dequeue the job with the given id from the given queue.
     *
     * @param queue Queue name.
     * @param id Id of the job.
     */
    public void dequeue(String queue, long id) {
        redis.execute((RedisConnection connection) -> {
            byte[] idBytes = value(id);
            connection.lRem(key(QUEUE, queue), 0, idBytes);
            connection.hDel(key(JOB, queue), idBytes);
            return null;
        });
    }

    //
    // Worker related.
    //

    /**
     * Pop first job from queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     * @return Job or null, if none is in the queue.
     */
    public Job pop(String queue, String worker) {
        return redis.execute((RedisConnection connection) -> {
            byte[] idBytes = connection.lPop(key(QUEUE, queue));
            if (idBytes == null) {
                return null;
            }
            connection.lPush(key(INFLIGHT, worker, queue), idBytes);

            byte[] jobBytes = connection.hGet(key(JOB, queue), idBytes);
            if (jobBytes == null) {
                return null;
            }

            return jobs.deserialize(jobBytes);
        });
    }

    /**
     * Remove job from inflight queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     */
    public void removeInflight(String queue, String worker) {
        redis.execute((RedisConnection connection) -> {
            connection.lPop(key(INFLIGHT, worker, queue));
            return null;
        });
    }

    /**
     * Restore job from inflight queue.
     *
     * @param queue Queue name.
     * @param worker Name of worker.
     */
    public void restoreInflight(String queue, String worker) {
        redis.execute((RedisConnection connection) -> {
            byte[] idBytes = connection.lPop(key(INFLIGHT, worker, queue));
            if (idBytes != null) {
                connection.lPush(key(QUEUE, queue), idBytes);
            }
            return null;
        });
    }

    //
    // Helper.
    //

    /**
     * Construct Redis key name. Created by joining the namespace and the parts together with ':'.
     *
     * @param parts Parts of the key name.
     */
    protected byte[] key(String... parts) {
        Assert.notEmpty(parts, "Precondition violated: parts are not empty.");
        return strings.serialize(Arrays.stream(parts).collect(Collectors.joining(":", namespace + ":", "")));
    }

    /**
     * Serialize long value.
     *
     * @param value Long.
     * @return Serialized long.
     */
    protected byte[] value(long value) {
        return value(Long.toString(value));
    }

    /**
     * Serialize string value.
     *
     * @param value String.
     * @return Serialized string.
     */
    protected byte[] value(String value) {
        return strings.serialize(value);
    }

    //
    // Injections.
    //

    /**
     * {@link RedisConnectionFactory} to access Redis.
     */
    public RedisConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * {@link RedisConnectionFactory} to access Redis.
     */
    public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * JSON mapper.
     */
    public ObjectMapper getJson() {
        return json;
    }

    /**
     * JSON mapper.
     */
    public void setJson(ObjectMapper json) {
        this.json = json;
    }
}
