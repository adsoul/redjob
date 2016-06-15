package com.s24.redjob.worker;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * Base implementation of {@link Worker} for queues.
 */
public abstract class AbstractQueueWorker extends AbstractWorker implements Runnable {
   /**
    * Queues to listen to.
    */
   private List<String> queues;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notEmpty(queues, "Precondition violated: queues not empty.");

      super.afterPropertiesSet();
   }

   /**
    * Create name for this worker.
    */
   protected String createName() {
      return super.createName() + ":" + StringUtils.collectionToCommaDelimitedString(queues);
   }

   @Override
   public void run() {
      try {
         MDC.put("worker", getName());
         workerDao.start(name);
         eventBus.publishEvent(new WorkerStart(this));
         poll();
      } catch (Throwable t) {
         log.error("Uncaught exception in worker. Worker stopped.", name, t);
         eventBus.publishEvent(new WorkerError(this, t));
      } finally {
         eventBus.publishEvent(new WorkerStopped(this));
         workerDao.stop(name);
      }
   }

   /**
    * Main poll loop.
    */
   protected void poll() {
      while (run.get()) {
         try {
            pollQueues();
         } catch (InterruptedException e) {
            // Just to be sure clear interrupt flag before starting over (if worker has not been requested to stop).
            Thread.interrupted();
            log.debug("Thread has been interrupted.", name);
         } catch (Throwable e) {
            log.error("Polling queues for jobs failed.", name, e);
         }
      }
   }

   /**
    * Poll all queues.
    *
    * @throws Throwable
    *            In case of errors.
    */
   protected void pollQueues() throws Throwable {
      for (String queue : queues) {
         try {
            MDC.put("queue", queue);
            boolean executed = pollQueue(queue);
            if (executed) {
               // Start over with polling.
               return;
            }
         } finally {
            MDC.remove("queue");
         }
      }
      Thread.sleep(emptyQueuesSleepMillis);
   }

   /**
    * Poll the given queue.
    *
    * @param queue
    *           Queue name.
    * @return Has a job been polled and executed?.
    * @throws Throwable
    *            In case of errors.
    */
   protected boolean pollQueue(String queue) throws Throwable {
      WorkerPoll workerPoll = new WorkerPoll(this, queue);
      eventBus.publishEvent(workerPoll);
      if (workerPoll.isVeto()) {
         log.debug("Queue poll vetoed.");
         return false;
      }

      Execution execution = doPollQueue(queue);
      if (execution == null) {
         log.debug("Queue is empty.");
         return false;
      }

      try {
         MDC.put("execution", Long.toString(execution.getId()));
         MDC.put("job", execution.getJob().getClass().getSimpleName());
         process(queue, execution);
         return true;

      } finally {
         try {
            removeInflight(queue);
         } finally {
            MDC.remove("job");
            MDC.remove("execution");
         }
      }
   }

   /**
    * Poll the given queue.
    *
    * @param queue
    *           Queue name.
    * @return Execution or null, if queue is empty.
    * @throws Throwable
    *            In case of errors.
    */
   protected abstract Execution doPollQueue(String queue) throws Throwable;

   /**
    * Remove executed (or maybe aborted) job from the inflight queue.
    *
    * @param queue
    *           Queue name.
    * @throws Throwable
    *            In case of errors.
    */
   protected abstract void removeInflight(String queue) throws Throwable;

   /**
    * Update execution.
    *
    * @param execution
    *           Execution.
    */
   public abstract void update(Execution execution);

   //
   // Injections.
   //

   /**
    * Queues to listen to.
    */
   public List<String> getQueues() {
      return queues;
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(String... queues) {
      setQueues(Arrays.asList(queues));
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(List<String> queues) {
      this.queues = queues;
   }
}
