package com.s24.redjob.channel;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.Message;
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
    * Redis serializer for job executions.
    */
   private final Jackson2JsonRedisSerializer<Execution> executions = new Jackson2JsonRedisSerializer<>(Execution.class);

   /**
    * Redis access.
    */
   private RedisTemplate<String, String> redis;

   @Override
   @PostConstruct
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      executions.setObjectMapper(json);

      redis = new RedisTemplate<>();
      redis.setConnectionFactory(connectionFactory);
      redis.setKeySerializer(strings);
      redis.setValueSerializer(executions);
      redis.afterPropertiesSet();
   }

   @Override
   public Execution publish(String channel, Object job) {
      return redis.execute((RedisConnection connection) -> {
         // Admin jobs do not use ids.
         Execution execution = new Execution(0, job);

         connection.publish(key(CHANNEL, channel), executions.serialize(execution));

         return execution;
      });
   }

   @Override
   public String getChannel(Message message) {
      return strings.deserialize(message.getChannel());
   }

   @Override
   public Execution getExecution(Message message) {
      return executions.deserialize(message.getBody());
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
