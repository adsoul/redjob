package com.s24.redjob.worker.json;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.queue.TestJob;

/**
 * Test of {@link TypeScanner}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TypeScannerTest {
   /**
    * JSON mapper.
    */
   @Spy
   private ObjectMapper objectMapper;

   /**
    * Redis serializer for job executions.
    */
   @Spy
   @InjectMocks
   private ExecutionRedisSerializer executions;

   @Test
   public void afterPropertiesSet() {
      scanForJsonSubtypes(executions, TestJob.class);

      verify(objectMapper).registerSubtypes(TestJob.class);
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param executions
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackage
    *           Class in package to scan recursively.
    */
   public static void scanForJsonSubtypes(ExecutionRedisSerializer executions, Class<?> basePackage) {
      scanForJsonSubtypes(executions, basePackage.getPackage());
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param executions
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackage
    *           Package to scan recursively.
    */
   public static void scanForJsonSubtypes(ExecutionRedisSerializer executions, Package basePackage) {
      scanForJsonSubtypes(executions, basePackage.getName());
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param executions
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackages
    *           Base packages to scan.
    */
   public static void scanForJsonSubtypes(ExecutionRedisSerializer executions, String... basePackages) {
      TypeScanner scanner = new TypeScanner();
      scanner.setExecutions(executions);
      scanner.setBasePackages(basePackages);
      scanner.afterPropertiesSet();
   }
}
