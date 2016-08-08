package com.s24.redjob.worker.json;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonParser;

/**
 * Test for {@link InstantDeserializer}.
 */
@RunWith(MockitoJUnitRunner.class)
public class InstantDeserializerTest {
   @Mock
   private JsonParser jp;

   @Test
   public void deserializer() throws Exception {
      InstantDeserializer deserializer = new InstantDeserializer();

      when(jp.getValueAsLong()).thenReturn(12345678L);

      assertEquals(Instant.ofEpochSecond(12345678L), deserializer.deserialize(jp, null));
   }
}
