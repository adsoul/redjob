package com.s24.redjob.worker.json;

import java.io.IOException;
import java.time.Instant;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * JSON serialization of {@link Instant}.
 */
@Component
public class InstantDeserializer extends JsonDeserializer<Instant> {
   @Override
   public Instant deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return Instant.ofEpochSecond(jp.getValueAsLong());
   }
}
