package com.s24.redjob.worker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * {@link JobRunnerFactory} using a Spring application context to retrieve job runners by the {@link JsonTypeName} of
 * jobs. The job runners are expected to be implement {@link Runnable} and to be prototypes providing a one argument
 * constructor accepting the job. Because they are prototypes, job runners are allowed to have state.
 */
public class SpringJobRunnerFactory implements JobRunnerFactory {
   /**
    * Bean factory.
    */
   @Autowired
   private ConfigurableListableBeanFactory beanFactory;

   @Override
   public Runnable runnerFor(Object job) {
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
      BeanDefinition beanDefinition = beanFactory.getBeanDefinition(jsonType);
      if (!BeanDefinition.SCOPE_PROTOTYPE.equals(beanDefinition.getScope())) {
         throw new IllegalArgumentException(String.format(
               "Job runners have to be prototypes, but job runner %s has scope %s.",
               jsonType, beanDefinition.getScope()));
      }

      Object runner = beanFactory.getBean(jsonType, job);
      if (!(runner instanceof Runnable)) {
         throw new IllegalArgumentException(String.format(
               "Job runners need to be Runnable, but job runner %s (%s) is not.",
               jsonType, runner.getClass().getName()));
      }

      return (Runnable) runner;
   }
}
