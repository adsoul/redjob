package com.s24.redjob.worker;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link JobRunnerFactory} using a Spring application context to retrieve job runners implementing {@link JobRunner}.
 * The job runners are expected to be prototypes. Because they are prototypes, job runners are allowed to have state.
 */
public class InterfaceJobRunnerFactory implements JobRunnerFactory, BeanFactoryAware {
   /**
    * Bean factory.
    */
   private ConfigurableListableBeanFactory beanFactory;

   @Override
   public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
      this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
   }

   @Override
   public <J> Runnable runnerFor(J job) {
      Assert.notNull(job, "Pre-condition violated: job != null.");

      ResolvableType type = ResolvableType.forClassWithGenerics(JobRunner.class, job.getClass());
      String[] beanNames = beanFactory.getBeanNamesForType(type);
      if (beanNames.length == 0) {
         throw new IllegalArgumentException(String.format(
               "No job runner found for %s.",
               job.getClass().getSimpleName()));
      }
      if (beanNames.length > 1) {
         throw new IllegalArgumentException(String.format(
               "More than one job runner found for %s: %s.",
               job.getClass().getSimpleName(), StringUtils.arrayToCommaDelimitedString(beanNames)));
      }
      String beanName = beanNames[0];

      // Check for bean existence and that bean is a prototype.
      // Beans have to be prototypes, because they are configured with the job vars below.
      BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
      if (!BeanDefinition.SCOPE_PROTOTYPE.equals(beanDefinition.getScope())) {
         throw new IllegalArgumentException(String.format(
               "Job runners have to be prototypes, but job runner %s has scope %s.",
               beanName, beanDefinition.getScope()));
      }

      JobRunner<J> runner = (JobRunner<J>) beanFactory.getBean(beanName);

      return new WrappingRunnable(runner) {
         @Override
         public void run() {
            runner.execute(job);
         }
      };
   }
}
