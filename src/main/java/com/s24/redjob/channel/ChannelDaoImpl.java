package com.s24.redjob.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.AbstractDao;
import com.s24.redjob.queue.Job;
import com.s24.redjob.queue.QueueDao;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;

import javax.annotation.PostConstruct;

/**
 * Default implementation of {@link QueueDao}.
 */
public class ChannelDaoImpl extends AbstractDao implements ChannelDao {
    /**
     * Redis key part for a job channel.
     */
    public static final String CHANNEL = "channel";

    /**
     * JSON mapper.
     */
    private ObjectMapper json = new ObjectMapper();

    /**
     * Redis serializer for jobs.
     */
    private final Jackson2JsonRedisSerializer<Job> jobs = new Jackson2JsonRedisSerializer<>(Job.class);

    /**
     * Redis access.
     */
    private RedisTemplate<String, String> redis;

    @Override
    @PostConstruct
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        jobs.setObjectMapper(json);

        redis = new RedisTemplate<>();
        redis.setConnectionFactory(connectionFactory);
        redis.setKeySerializer(strings);
        redis.setValueSerializer(jobs);
        redis.afterPropertiesSet();
    }

    @Override
    public void publish(String channel, Object payload) {
        redis.execute((RedisConnection connection) -> {
            // Admin jobs do not use ids.
            Job job = new Job(0, payload);

            connection.publish(key(CHANNEL, channel), jobs.serialize(job));

            return null;
        });
    }

    //
    // Injections.
    //

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
