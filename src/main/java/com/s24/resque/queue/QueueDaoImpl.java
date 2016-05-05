package com.s24.resque.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Dao for accessing job queues.
 */
public class QueueDaoImpl {
    /**
     * Log.
     */
    private static final Log log = LogFactory.getLog(QueueDaoImpl.class);

    /**
     * Redis key part for id sequence.
     */
    public static final String ID = "id";

    /**
     * Redis key part for set of all queue names.
     */
    public static final String QUEUES = "queues";

    /**
     * Redis key part for list of all jobs of a queue.
     */
    public static final String QUEUE = "queue";

    /**
     * {@link RedisConnectionFactory} to access Redis.
     */
    private RedisConnectionFactory connectionFactory;

    /**
     * Redis serializer for strings.
     */
    private final StringRedisSerializer string = new StringRedisSerializer();

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

    @PostConstruct
    public void afterPropertiesSet() {
        Assert.notNull(connectionFactory, "Precondition violated: connectionFactory != null.");
        Assert.hasLength(namespace, "Precondition violated: namespace has length.");
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
        return execute(connection -> {
            Long id = connection.incr(key(ID));
            Job job = new Job(id, payload);

            connection.sAdd(key(QUEUES), value(queue));
            byte[] queueBytes = key(QUEUE, queue);
            byte[] jobBytes = toJson(job);
            if (front) {
                connection.lPush(queueBytes, jobBytes);
            } else {
                connection.rPush(queueBytes, jobBytes);
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
        execute(connection -> {
            byte[] jobBytes = null;
            connection.lRange(key(QUEUE, queue), 0, -1);
            connection.lRem(key(QUEUE, queue), 1, jobBytes);
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
     * @return Job or null, if none is in the queue.
     */
    public Job pop(String queue) {
        return execute(connection -> fromJson(connection.lPop(key(QUEUE, queue))));
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
        return string.serialize(Arrays.stream(parts).collect(Collectors.joining(":", namespace + ":", "")));
    }

    /**
     * Serialize string value.
     *
     * @param value String.
     * @return Serialized string.
     */
    protected byte[] value(String value) {
        return string.serialize(value);
    }

    /**
     * Serialize job.
     */
    protected byte[] toJson(Job job) throws JsonProcessingException {
        byte[] jobBytes = json.writeValueAsBytes(job);

        if (log.isDebugEnabled()) {
            log.debug("Serialized job: " + new String(jobBytes, StandardCharsets.UTF_8));
        }

        return jobBytes;
    }

    /**
     * Deserialize job. Null-safe.
     *
     * @param jobBytes Serialized job.
     * @return Deserialized job.
     * @throws IOException In case of any IO errors.
     */
    protected Job fromJson(byte[] jobBytes) throws IOException {
        if (jobBytes == null) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Deserialize job: " + new String(jobBytes, StandardCharsets.UTF_8));
        }

        return json.readValue(jobBytes, Job.class);
    }

    /**
     * Execute commands NOT pipelined.
     *
     * @param commands Redis commands.
     * @param <R> Type of result.
     * @return Result of commands.
     */
    protected <R> R execute(RedisCommands<R> commands) {
        Assert.notNull(commands, "Precondition violated: commands != null.");

        RedisConnection connection = RedisConnectionUtils.getConnection(connectionFactory);
        try {
            return commands.execute(connection);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON serialization problems.", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("IO problems.", e);
        } finally {
            RedisConnectionUtils.releaseConnection(connection, connectionFactory);
        }
    }

    /**
     * Redis commands.
     *
     * @param <R> Type of commands result.
     */
    protected static interface RedisCommands<R> {
        /**
         * Execute commands.
         *
         * @param connection Redis connection.
         * @return Result of commands.
         */
        R execute(RedisConnection connection) throws IOException;
    }

    //
    // Injections
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
}
