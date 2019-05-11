package com.adsoul.redjob.listener;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.util.Assert;

import com.adsoul.redjob.queue.QueueWorker;
import com.adsoul.redjob.worker.Execution;
import com.adsoul.redjob.worker.events.JobEvent;
import com.adsoul.redjob.worker.events.JobFailure;
import com.adsoul.redjob.worker.events.JobStart;
import com.adsoul.redjob.worker.events.JobSuccess;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

/**
 * {@link QueueWorker} event listener for regularly updating job results.
 * So the client is able to see progress during job execution.
 *
 * Job results need to be thread safe for this to work reliably.
 */
@Lazy(false)
public class ExecutionResultUpdateListener {
   /**
    * Logger.
    */
   protected final Logger log = LoggerFactory.getLogger(getClass());

   /**
    * Update interval in milliseconds.
    */
   private long updateIntervalMillis = 1000;

   /**
    * Currently executing jobs.
    */
   private final Map<Long, Entry<QueueWorker, Execution>> executions = new ConcurrentHashMap<>();

   /**
    * Job result updater.
    */
   private final Timer timer = new Timer("Job result updater", true);

   @PostConstruct
   public void afterPropertiesSet() {
      timer.scheduleAtFixedRate(new TimerTask() {
         @Override
         public void run() {
            if (executions.isEmpty()) {
               return;
            }

            for (Entry<QueueWorker, Execution> entry : executions.values()) {
               QueueWorker worker = entry.getKey();
               Execution execution = entry.getValue();
               update(worker, execution);
            }
            handleUpdates(executions.values().stream().map(Entry::getValue).collect(toList()));
         }
      }, updateIntervalMillis, updateIntervalMillis);
   }

   @PreDestroy
   private void shutdown() {
      timer.cancel();
   }

   /**
    * Completely ignore the job in this listener?.
    *
    * @param event
    *           Job event.
    */
   protected boolean ignoreJob(JobEvent event) {
      return false;
   }

   /**
    * On job start register for updates.
    *
    * @param event
    *           Job start event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.adsoul.redjob.queue.QueueWorker)")
   public void onJobStart(JobStart event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      QueueWorker worker = event.getWorker();
      Execution execution = event.getExecution();

      handleJobStart(event);
      // Register execution for updates.
      executions.put(execution.getId(), new SimpleImmutableEntry<>(worker, execution));
      handleUpdate(execution);
   }

   /**
    * Add custom functionality on job start.
    *
    * @param event
    *           Job start event.
    */
   protected void handleJobStart(JobStart event) {
   }

   /**
    * On job success remove job result.
    *
    * @param event
    *           Job success event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.adsoul.redjob.queue.QueueWorker)")
   public void onJobSuccess(JobSuccess event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      QueueWorker worker = event.getWorker();
      Execution execution = event.getExecution();

      Entry<QueueWorker, Execution> entry = executions.remove(execution.getId());
      if (entry == null) {
         return;
      }

      handleJobSuccess(event);
      update(worker, execution);
      handleUpdate(execution);
   }

   /**
    * Add custom functionality on job success.
    *
    * @param event
    *           Job success event.
    */
   protected void handleJobSuccess(JobSuccess event) {
   }

   /**
    * On job failure remove job result.
    *
    * @param event
    *           Job failure event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.adsoul.redjob.queue.QueueWorker)")
   public void onJobFailure(JobFailure event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      QueueWorker worker = event.getWorker();
      Execution execution = event.getExecution();

      Entry<QueueWorker, Execution> entry = executions.remove(execution.getId());
      if (entry == null) {
         return;
      }

      handleJobFailure(event);
      update(worker, execution);
      handleUpdate(execution);
   }

   /**
    * Add custom functionality on job failure.
    *
    * @param event
    *           Job failure event.
    */
   protected void handleJobFailure(JobFailure event) {
   }

   /**
    * Update execution.
    *
    * @param worker
    *           Worker.
    * @param execution
    *           Execution.
    */
   protected void update(QueueWorker worker, Execution execution) {
      Assert.notNull(worker, "Pre-condition violated: worker != null.");
      Assert.notNull(execution, "Pre-condition violated: execution != null.");

      try {
         worker.update(execution);
      } catch (Exception e) {
         log.error("Failed to update job results.", e);
      }
   }

   /**
    * Add custom functionality on update of {@link Execution}.
    *
    * @param execution
    *           Execution.
    */
   protected final void handleUpdate(Execution execution) {
      handleUpdates(singleton(execution));
   }

   /**
    * Add custom functionality on update of {@link Execution}s.
    *
    * @param executions
    *           Executions.
    */
   protected void handleUpdates(Collection<Execution> executions) {
   }

   //
   // Injections.
   //

   /**
    * Update interval in milliseconds.
    */
   public long getUpdateIntervalMillis() {
      return updateIntervalMillis;
   }

   /**
    * Update interval in milliseconds.
    */
   public void setUpdateIntervalMillis(long updateIntervalMillis) {
      this.updateIntervalMillis = updateIntervalMillis;
   }
}
