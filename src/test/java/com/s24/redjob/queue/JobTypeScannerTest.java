package com.s24.redjob.queue;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.s24.redjob.worker.JobTypeScanner;

/**
 * Test of {@link JobTypeScanner}.
 */
@RunWith(MockitoJUnitRunner.class)
public class JobTypeScannerTest {
   /**
    * JSON mapper.
    */
   @Spy
   private ObjectMapper json;

   @Test
   public void afterPropertiesSet() {
      scanForJsonSubtypes(json, TestJob.class);

      verify(json).registerSubtypes(TestJob.class);
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param json
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackage
    *           Class in package to scan recursively.
    */
   public static void scanForJsonSubtypes(ObjectMapper json, Class<?> basePackage) {
      scanForJsonSubtypes(json, basePackage.getPackage());
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param json
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackage
    *           Package to scan recursively.
    */
   public static void scanForJsonSubtypes(ObjectMapper json, Package basePackage) {
      scanForJsonSubtypes(json, basePackage.getName());
   }

   /**
    * Scan packages for JSON subtypes. Useful for tests.
    *
    * @param json
    *           JSON mapper to register subtypes (classes annotated with {@link JsonTypeName}) at.
    * @param basePackages
    *           Base packages to scan.
    */
   public static void scanForJsonSubtypes(ObjectMapper json, String... basePackages) {
      JobTypeScanner scanner = new JobTypeScanner();
      scanner.setJson(json);
      scanner.setBasePackages(basePackages);
      scanner.afterPropertiesSet();
   }
}
