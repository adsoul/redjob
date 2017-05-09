package com.s24.redjob.worker;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * {@link JobRunnerFactory} using a Spring application context to retrieve job runners by the {@link JsonTypeName} of
 * jobs. The job runners are expected to be implement {@link Runnable} and to be prototypes providing a one argument
 * constructor accepting the job. Because they are prototypes, job runners are allowed to have state.
 */
public class JsonTypeJobRunnerFactory implements JobRunnerFactory, ApplicationContextAware {
   /**
    * Application context.
    */
   private ApplicationContext applicationContext;

   @Override
   public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
      this.applicationContext = applicationContext;
   }

   @Override
   public <J> Runnable runnerFor(J job) {
      Assert.notNull(job, "Pre-condition violated: job != null.");

      JsonTypeName type = job.getClass().getAnnotation(JsonTypeName.class);
      Assert.notNull(type, "Pre-condition violated: Job has a @JsonTypeName.");
      String jsonType = type.value();
      if (!StringUtils.hasLength(jsonType)) {
         // If the type is not specified in the annotation, use the simple class name as documented.
         jsonType = job.getClass().getSimpleName();
      }

      // Check for bean existence and that bean is a prototype.
      // Beans have to be prototypes, because they are configured with the job vars below.
      if (!applicationContext.isPrototype(jsonType)) {
         throw new IllegalArgumentException(String.format(
               "Job runners have to be prototypes, but job runner %s is none.", jsonType));
      }

      Object runner = applicationContext.getBean(jsonType, job);
      if (!(runner instanceof Runnable)) {
         throw new IllegalArgumentException(String.format(
               "Job runners need to be Runnable, but job runner %s (%s) is not.",
               jsonType, runner.getClass().getName()));
      }

      return (Runnable) runner;
   }
}
