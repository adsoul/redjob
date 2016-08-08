package com.s24.redjob.worker.json;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Instant;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Test for {@link InstantSerializer}.
 */
@RunWith(MockitoJUnitRunner.class)
public class InstantSerializerTest {
   @Mock
   private JsonGenerator jgen;

   @Test
   public void serialize() throws Exception {
      InstantSerializer serializer = new InstantSerializer();

      serializer.serialize(Instant.ofEpochSecond(12345678L), jgen, null);

      verify(jgen).writeNumber(12345678L);
      verifyNoMoreInteractions(jgen);
   }
}
