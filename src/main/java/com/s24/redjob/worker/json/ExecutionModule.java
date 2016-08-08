package com.s24.redjob.worker.json;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.GenericTypeResolver;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.s24.redjob.worker.Execution;

/**
 * Jackson module with all serializers needed for {@link Execution} and containing classes.
 */
public class ExecutionModule extends SimpleModule {
   /**
    * All serializer beans.
    */
   @Autowired
   private List<JsonSerializer<?>> serializers;

   /**
    * All deserializer beans.
    */
   @Autowired
   private List<JsonDeserializer<?>> deserializers;

   /**
    * Constructor.
    */
   public ExecutionModule() {
      super("Execution module");
   }

   /**
    * Register all serializer and deserializer of this package.
    */
   @PostConstruct
   public void afterPropertiesSet() {
      addAllOf(ExecutionModule.class.getPackage());
   }

   /**
    * Add all {@link JsonSerializer} and {@link JsonDeserializer} beans of the given package.
    */
   protected void addAllOf(Package p) {
      serializers.stream()
            .filter(serializer -> serializer.getClass().getPackage().equals(p))
            .forEach(this::addTypedSerializer);
      deserializers.stream()
            .filter(deserializer -> deserializer.getClass().getPackage().equals(p))
            .forEach(this::addTypedDeserializer);
   }

   /**
    * Extract type from serializer and register it.
    */
   protected <T> void addTypedSerializer(JsonSerializer<T> serializer) {
      addSerializer(
            (Class<? extends T>) GenericTypeResolver.resolveTypeArgument(serializer.getClass(), JsonSerializer.class),
            serializer);
   }

   /**
    * Extract type from deserializer and register it.
    */
   protected <T> void addTypedDeserializer(JsonDeserializer<? extends T> deserializer) {
      addDeserializer(
            (Class<T>) GenericTypeResolver.resolveTypeArgument(deserializer.getClass(), JsonDeserializer.class),
            deserializer);
   }
}
