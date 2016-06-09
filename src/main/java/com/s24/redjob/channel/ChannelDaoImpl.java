package com.s24.redjob.channel;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.AbstractDao;
import com.s24.redjob.queue.QueueDao;
import com.s24.redjob.worker.Execution;

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
   private final Jackson2JsonRedisSerializer<Execution> jobs = new Jackson2JsonRedisSerializer<>(Execution.class);

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
   public void publish(String channel, Object job) {
      redis.execute((RedisConnection connection) -> {
         // Admin jobs do not use ids.
         Execution execution = new Execution(0, job);

         connection.publish(key(CHANNEL, channel), jobs.serialize(execution));

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
