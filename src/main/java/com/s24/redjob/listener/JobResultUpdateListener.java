package com.s24.redjob.listener;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.util.Assert;

import com.s24.redjob.queue.QueueWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.events.JobEvent;
import com.s24.redjob.worker.events.JobFailure;
import com.s24.redjob.worker.events.JobStart;
import com.s24.redjob.worker.events.JobSuccess;

/**
 * {@link QueueWorker} event listener for regularly updating job results.
 * So the client is able to see progress during job execution.
 */
@Lazy(false)
public class JobResultUpdateListener {
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
               try {
                  update(worker, execution);
               } catch (Exception e) {
                  log.error("Failed to update job results.", e);
               }
            }
         }
      }, updateIntervalMillis, updateIntervalMillis);
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

      worker.update(execution);
   }

   @PreDestroy
   private void shutdown() {
      timer.cancel();
   }

   /**
    * On job start register for updates.
    *
    * @param event
    *           Job start event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
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
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
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
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
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
    * Completely ignore the job in this listener?.
    *
    * @param event
    *           Job event.
    */
   protected boolean ignoreJob(JobEvent event) {
      return false;
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
