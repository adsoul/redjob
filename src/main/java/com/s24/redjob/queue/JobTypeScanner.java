package com.s24.redjob.queue;

import javax.annotation.PostConstruct;

import org.springframework.data.util.AnnotatedTypeScanner;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Scans the classpath and registers JSON subtypes (@link {@link JsonTypeName}) at a {@link ObjectMapper}.
 */
public class JobTypeScanner extends AnnotatedTypeScanner {
   /**
    * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
    */
   private ObjectMapper json;

   /**
    * Base packages to scan.
    */
   private String[] basePackages;

   @PostConstruct
   public void afterPropertiesSet() {
      Assert.notNull(json, "Precondition violated: json != null.");
      Assert.notNull(basePackages, "Precondition violated: basePackages != null.");

      findTypes(basePackages).forEach(json::registerSubtypes);
   }

   /**
    * Constructor.
    */
   public JobTypeScanner() {
      super(JsonTypeName.class);
   }

   /**
    * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
    */
   public ObjectMapper getJson() {
      return json;
   }

   /**
    * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
    */
   public void setJson(ObjectMapper json) {
      this.json = json;
   }

   /**
    * Base packages to scan.
    */
   public String[] getBasePackages() {
      return basePackages;
   }

   /**
    * Base packages to scan.
    */
   public void setBasePackages(String... basePackages) {
      this.basePackages = basePackages;
   }
}
