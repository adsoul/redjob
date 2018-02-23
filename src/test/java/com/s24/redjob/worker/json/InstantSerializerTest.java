package com.s24.redjob.worker.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.s24.redjob.mockito.EnableMockito;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.time.Instant;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Test for {@link InstantSerializer}.
 */
@EnableMockito
class InstantSerializerTest {
   @Mock
   private JsonGenerator jgen;

   @Test
   void serialize() throws Exception {
      InstantSerializer serializer = new InstantSerializer();

      serializer.serialize(Instant.ofEpochSecond(12345678L), jgen, null);

      verify(jgen).writeNumber(12345678L);
      verifyNoMoreInteractions(jgen);
   }
}
