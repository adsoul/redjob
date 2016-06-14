package com.s24.redjob.worker;

import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * {@link RedisSerializer} for {@link Execution}s.
 */
public class ExecutionRedisSerializer extends Jackson2JsonRedisSerializer<Execution> {
   /**
    * Object mapper.
    */
   private ObjectMapper objectMapper = new ObjectMapper();

   /**
    * Constructor.
    */
   public ExecutionRedisSerializer() {
      super(Execution.class);
      setObjectMapper(objectMapper);
   }

   /**
    * Object mapper.
    */
   public ObjectMapper getObjectMapper() {
      return objectMapper;
   }

   @Override
   public void setObjectMapper(ObjectMapper objectMapper) {
      objectMapper.registerSubtypes(Result.class);
      super.setObjectMapper(objectMapper);
      this.objectMapper = objectMapper;
   }
}
