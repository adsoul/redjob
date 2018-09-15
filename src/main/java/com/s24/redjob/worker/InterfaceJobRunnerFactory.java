package com.s24.redjob.worker;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link JobRunnerFactory} using a Spring application context to retrieve job runners implementing {@link JobRunner}.
 * The job runners are expected to be prototypes. Because they are prototypes, job runners are allowed to have state.
 */
public class InterfaceJobRunnerFactory implements JobRunnerFactory, ApplicationContextAware {
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

      ResolvableType type = ResolvableType.forClassWithGenerics(JobRunner.class, job.getClass());
      String[] beanNames = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(applicationContext, type);
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
      if (!applicationContext.isPrototype(beanName)) {
         throw new IllegalArgumentException(String.format(
               "Job runners have to be prototypes, but job runner %s is none.", beanName));
      }

      @SuppressWarnings("unchecked")
      JobRunner<J> runner = (JobRunner<J>) applicationContext.getBean(beanName);
      Assert.notNull(runner, "Pre-condition violated: runner != null.");

      return new WrappingRunnable(runner) {
         @Override
         public void run() {
            runner.execute(job);
         }
      };
   }
}
