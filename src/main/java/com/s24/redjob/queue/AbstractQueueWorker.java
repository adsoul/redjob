package com.s24.redjob.queue;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.worker.AbstractWorker;
import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.Worker;
import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerNext;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * Base implementation of {@link Worker} for queues.
 */
public abstract class AbstractQueueWorker extends AbstractWorker implements Runnable, QueueWorker {
   /**
    * Queues to listen to.
    */
   private List<String> queues;

   /**
    * Currently processed execution, if any.
    */
   private volatile Execution execution = new Execution(-1, "dummy");

   /**
    * Worker thread.
    */
   private Thread thread;

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
   public void start() {
      thread = new Thread(this, getName());
      thread.start();
   }

   @Override
   public void destroy() {
      super.destroy();
      log.info("Waiting for worker to shutdown.");
      if (thread != null) {
         try {
            thread.interrupt();
            thread.join();
         } catch (InterruptedException e) {
            // Ignore
         }
      }
      log.info("Worker has been shut down.");
   }

   @Override
   public void run() {
      try {
         MDC.put("worker", getName());
         log.info("Starting worker {}.", getName());
         workerDao.start(name);
         eventBus.publishEvent(new WorkerStart(this));
         startup();
         poll();
      } catch (Throwable t) {
         log.error("Uncaught exception in worker. Worker stopped.", name, t);
         eventBus.publishEvent(new WorkerError(this, t));
      } finally {
         log.info("Stop worker {}.", getName());
         eventBus.publishEvent(new WorkerStopped(this));
         workerDao.stop(name);
      }
   }

   /**
    * Startup initialization.
    */
   protected void startup() throws Throwable {
      for (String queue : queues) {
         restoreInflight(queue);
      }
   }

   /**
    * Main poll loop.
    */
   protected void poll() {
      while (shouldRun.get()) {
         try {
            pollQueues();
         } catch (InterruptedException e) {
            // Just to be sure clear interrupt flag before starting over (if worker has not been requested to stop).
            Thread.interrupted();
            log.debug("Thread has been interrupted.");
         } catch (Throwable e) {
            log.error("Polling queues for jobs failed.", e);
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
            WorkerPoll workerPoll = new WorkerPoll(this, queue);
            eventBus.publishEvent(workerPoll);
            if (workerPoll.isVeto()) {
               log.debug("Queue poll vetoed.");
            } else if (pollQueue(queue)) {
               // Event popped and executed -> Start over with polling.
               return;
            }
         } finally {
            eventBus.publishEvent(new WorkerNext(this, queue));
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
      Execution execution = doPollQueue(queue);
      if (execution == null) {
         log.debug("Queue is empty.");
         return false;
      }

      boolean restore = false;
      try {
         this.execution = execution;
         MDC.put("execution", Long.toString(execution.getId()));
         MDC.put("job", execution.getJob().getClass().getSimpleName());
         restore = process(queue, execution);
         return true;

      } catch (Throwable t) {
         log.error("Job processing failed.", t);
         return true;

      } finally {
         synchronized (this.execution) {
            this.execution = null;
         }
         try {
            if (restore) {
               restoreInflight(queue);
            } else {
               removeInflight(queue);
            }
         } finally {
            MDC.remove("job");
            MDC.remove("execution");
         }
      }
   }

   @Override
   public void stop(long id) {
      synchronized (this.execution) {
         if (execution.getId() == id) {
            thread.interrupt();
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
    * Restore skipped job from the inflight queue.
    *
    * @param queue
    *           Queue name.
    * @throws Throwable
    *            In case of errors.
    */
   protected abstract void restoreInflight(String queue) throws Throwable;

   @Override
   protected void run(String queue, Execution execution, Runnable runner, Object unwrappedRunner) {
      try {
         // Save start time.
         update(execution);
         super.run(queue, execution, runner, unwrappedRunner);
      } finally {
         // Save stop time.
         update(execution);
      }
   }

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
