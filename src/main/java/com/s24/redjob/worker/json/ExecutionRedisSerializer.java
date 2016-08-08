package com.s24.redjob.worker.json;

import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.NoResult;

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
      objectMapper.registerSubtypes(NoResult.class);
      super.setObjectMapper(objectMapper);
      this.objectMapper = objectMapper;
   }

   /**
    * Add additional sub types to the object mapper.
    */
   public void setTypes(Class... types) {
      objectMapper.registerSubtypes(types);
   }

   /**
    * Add additional modules to the object mapper.
    */
   public void setModules(Module... modules) {
      objectMapper.registerModules(modules);
   }
}
