package com.adsoul.redjob.worker.json;

import java.io.IOException;
import java.time.Instant;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * JSON serialization of {@link Instant}.
 */
@Component
public class InstantSerializer extends JsonSerializer<Instant> {
   @Override
   public void serialize(Instant value, JsonGenerator generator, SerializerProvider provider) throws IOException {
      generator.writeNumber(value.getEpochSecond());
   }
}
