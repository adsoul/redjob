package com.s24.redjob.worker.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.NoResult;

/**
 * {@link RedisSerializer} for {@link Execution}s.
 */
public class ExecutionRedisSerializer extends Jackson2JsonRedisSerializer<Execution> {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(ExecutionRedisSerializer.class);

   /**
    * Object mapper.
    */
   private ObjectMapper objectMapper = new ObjectMapper();

   /**
    * Ignore deserialization failures?.
    */
   private boolean ignoreDeserializationFailures = false;

   /**
    * Constructor.
    */
   public ExecutionRedisSerializer() {
      super(Execution.class);
      setObjectMapper(objectMapper);
   }

   @Override
   public Execution deserialize(byte[] bytes) throws SerializationException {
      try {
         return super.deserialize(bytes);
      } catch (SerializationException e) {
         if (ignoreDeserializationFailures) {
            log.warn("Ignoring invalid JSON: {}.", e.getMessage());
            return null;
         }

         throw e;
      }
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

   /**
    * Ignore deserialization failures?.
    */
   public void setIgnoreDeserializationFailures(boolean ignoreDeserializationFailures) {
      this.ignoreDeserializationFailures = ignoreDeserializationFailures;
   }
}
