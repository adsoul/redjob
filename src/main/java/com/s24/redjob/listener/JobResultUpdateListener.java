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
import com.s24.redjob.worker.events.JobFailed;
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
   private final Logger log = LoggerFactory.getLogger(getClass());

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
                  worker.update(execution);
               } catch (Exception e) {
                  log.error("Failed to update job results.", e);
               }
            }
         }
      }, updateIntervalMillis, updateIntervalMillis);
   }

   @PreDestroy
   private void shutdown() {
      timer.cancel();
   }

   /**
    * On job start register for updates.
    *
    * @param event
    *           Job execute event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
   public void onStart(JobStart event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      QueueWorker worker = event.getWorker();
      Execution execution = event.getExecution();
      handleStart(worker, execution);

      // Register execution for updates.
      executions.put(execution.getId(), new SimpleImmutableEntry<>(event.getWorker(), execution));
   }

   /**
    * Add custom functionality on job start.
    *
    * @param worker
    *           Worker.
    * @param execution
    *           Execution.
    */
   protected void handleStart(QueueWorker worker, Execution execution) {
   }

   /**
    * On job success remove job result.
    *
    * @param event
    *           Job success event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
   public void onSuccess(JobSuccess event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      Entry<QueueWorker, Execution> entry = executions.remove(event.getExecution().getId());
      if (entry == null) {
         return;
      }

      QueueWorker worker = entry.getKey();
      Execution execution = entry.getValue();
      handleSuccess(worker, execution);

      worker.update(execution);
   }

   /**
    * Add custom functionality on job success.
    *
    * @param worker
    *           Worker.
    * @param execution
    *           Execution.
    */
   protected void handleSuccess(QueueWorker worker, Execution execution) {
   }

   /**
    * On job failure remove job result.
    *
    * @param event
    *           Job failure event.
    */
   @EventListener(condition = "#event.worker instanceof T(com.s24.redjob.queue.QueueWorker)")
   public void onFailure(JobFailed event) {
      Assert.notNull(event, "Pre-condition violated: event != null.");
      if (ignoreJob(event)) {
         return;
      }

      Entry<QueueWorker, Execution> entry = executions.remove(event.getExecution().getId());
      if (entry == null) {
         return;
      }

      QueueWorker worker = entry.getKey();
      Execution execution = entry.getValue();
      handleFailure(worker, execution);

      worker.update(execution);
   }

   /**
    * Add custom functionality on job failure.
    *
    * @param worker
    *           Worker.
    * @param execution
    *           Execution.
    */
   protected void handleFailure(QueueWorker worker, Execution execution) {
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
