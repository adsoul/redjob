package com.s24.redjob.worker;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.util.AnnotatedTypeScanner;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Scans the classpath and registers JSON subtypes (@link {@link JsonTypeName}) at a {@link ObjectMapper}.
 */
public class JobTypeScanner extends AnnotatedTypeScanner {
   /**
    * Redis serializer for job executions.
    */
   @Autowired
   private ExecutionRedisSerializer executions;

   /**
    * Base packages to scan.
    */
   private String[] basePackages;

   @PostConstruct
   public void afterPropertiesSet() {
      Assert.notNull(executions, "Precondition violated: json != null.");
      Assert.notNull(basePackages, "Precondition violated: basePackages != null.");

      ObjectMapper objectMapper = executions.getObjectMapper();
      findTypes(basePackages).forEach(objectMapper::registerSubtypes);
   }

   /**
    * Constructor.
    */
   public JobTypeScanner() {
      super(JsonTypeName.class);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return executions;
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      this.executions = executions;
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
