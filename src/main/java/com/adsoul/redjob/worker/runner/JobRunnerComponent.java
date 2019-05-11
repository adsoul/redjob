package com.adsoul.redjob.worker.runner;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Meta annotation to mark a bean as a {@link JobRunner}:
 * {@link Component} with {@value ConfigurableBeanFactory#SCOPE_PROTOTYPE} {@link Scope}.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Component
public @interface JobRunnerComponent {
}
