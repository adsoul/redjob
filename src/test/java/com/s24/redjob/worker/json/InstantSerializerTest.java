package com.s24.redjob.worker.json;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Instant;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Test for {@link InstantSerializer}.
 */
public class InstantSerializerTest {
   @Rule
   public MockitoRule mockitoRule = MockitoJUnit.rule();
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
