package com.s24.redjob.channel;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.util.Assert;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.queue.FifoDao;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.json.ExecutionRedisSerializer;

/**
 * Default implementation of {@link FifoDao}.
 */
public class ChannelDaoImpl extends AbstractDao implements ChannelDao {
   /**
    * Redis key part for a job channel.
    */
   public static final String CHANNEL = "channel";

   /**
    * Redis serializer for job executions.
    */
   private ExecutionRedisSerializer executions;

   /**
    * Redis access.
    */
   private RedisTemplate<String, String> redis;

   @Override
   @PostConstruct
   public void afterPropertiesSet() {
      super.afterPropertiesSet();

      Assert.notNull(executions, "Precondition violated: executions != null.");

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
   public ChannelTopic getTopic(String channel) {
      return new ChannelTopic(keyString(CHANNEL, channel));
   }

   @Override
   public String getChannel(Message message) {
      String channel = strings.deserialize(message.getChannel());
      return channel.substring(channel.lastIndexOf(":") + 1);
   }

   @Override
   public Execution getExecution(Message message) {
      return executions.deserialize(message.getBody());
   }

   //
   // Injections.
   //

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return executions;
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      this.executions = executions;
   }
}
