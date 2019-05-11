package com.adsoul.redjob.worker.json;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.adsoul.redjob.mockito.EnableMockito;
import com.fasterxml.jackson.core.JsonParser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Test for {@link InstantDeserializer}.
 */
@EnableMockito
class InstantDeserializerTest {
   @Mock
   private JsonParser jp;

   @Test
   void deserializer() throws Exception {
      InstantDeserializer deserializer = new InstantDeserializer();

      when(jp.getValueAsLong()).thenReturn(12345678L);

      assertEquals(Instant.ofEpochSecond(12345678L), deserializer.deserialize(jp, null));
   }
}
