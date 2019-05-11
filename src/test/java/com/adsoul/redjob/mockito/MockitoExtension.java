package com.adsoul.redjob.mockito;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.junit.JUnitRule;
import org.mockito.junit.MockitoRule;

/**
 * JUnit 5 extension for Mockito.
 * Replacement for {@link MockitoRule} / {@link JUnitRule}.
 */
public class MockitoExtension implements Extension, TestInstancePostProcessor, AfterEachCallback {
   @Override
   public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
      MockitoAnnotations.initMocks(testInstance);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      Mockito.validateMockitoUsage();
   }
}
