package com.adsoul.redjob.worker.json;

import com.adsoul.redjob.worker.NoResult;

/**
 * {@link ExecutionRedisSerializer} ready configured for tests.
 */
public class TestExecutionRedisSerializer extends ExecutionRedisSerializer {
   /**
    * Constructor.
    */
   public TestExecutionRedisSerializer(Class<?> subType) {
      setModules(new ExecutionModule() {{
         addTypedSerializer(new InstantSerializer());
         addTypedDeserializer(new InstantDeserializer());
         registerSubtypes(subType, NoResult.class);
      }});
   }
}
