package com.s24.redjob.queue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.s24.redjob.worker.Execution;
import com.s24.redjob.worker.JobRunnerFactory;
import com.s24.redjob.worker.Worker;
import com.s24.redjob.worker.WorkerDao;
import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobFailed;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSkipped;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;

/**
 * Default implementation of {@link Worker}.
 */
public class QueueWorker implements Worker, Runnable, ApplicationEventPublisherAware {
   /**
    * Log.
    */
   private static final Logger log = LoggerFactory.getLogger(QueueWorker.class);

   /**
    * Queues to listen to.
    */
   private List<String> queues;

   /**
    * Queue dao.
    */
   private QueueDao queueDao;

   /**
    * Worker dao.
    */
   private WorkerDao workerDao;

   /**
    * Sequence for worker ids.
    */
   private static final AtomicInteger IDS = new AtomicInteger();

   /**
    * Worker id.
    */
   private int id;

   /**
    * Name of this worker.
    */
   private String name;

   /**
    * Factory for creating job runners.
    */
   private JobRunnerFactory jobRunnerFactory;

   /**
    * Default: Number of milliseconds the worker pauses, if none of the queues contained a job.
    */
   public static final int DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS = 500;

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   private long emptyQueuesSleepMillis = DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS;

   /**
    * Should worker run?. False stops this worker.
    */
   private AtomicBoolean run = new AtomicBoolean(true);

   /**
    * Event bus.
    */
   private ApplicationEventPublisher eventBus;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notEmpty(queues, "Precondition violated: queues not empty.");
      Assert.notNull(queueDao, "Precondition violated: queueDao != null.");
      Assert.notNull(jobRunnerFactory, "Precondition violated: jobRunnerFactory != null.");
      Assert.isTrue(emptyQueuesSleepMillis > 0, "Precondition violated: emptyQueuesSleepMillis > 0.");
      Assert.notNull(eventBus, "Precondition violated: eventBus != null.");

      id = IDS.incrementAndGet();
      name = createName();
   }

   /**
    * Create name for this worker.
    */
   protected String createName() {
      return ManagementFactory.getRuntimeMXBean().getName() + ":" + id + ":" +
            StringUtils.collectionToCommaDelimitedString(queues);
   }

   @Override
   public int getId() {
      return id;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public void stop() {
      run.set(false);
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

      Execution execution = queueDao.pop(queue, name);
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
         MDC.remove("job");
         MDC.remove("execution");
      }
   }

   /**
    * Process job.
    *
    * @param queue
    *           Name of queue.
    * @param execution
    *           Job.
    * @throws Throwable
    *            In case of errors.
    */
   protected void process(String queue, Execution execution) throws Throwable {
      Object job = execution.getJob();
      if (job == null) {
         log.error("Missing job.");
         throw new IllegalArgumentException("Missing job.");
      }

      JobProcess jobProcess = new JobProcess(this, queue, job);
      eventBus.publishEvent(jobProcess);
      if (jobProcess.isVeto()) {
         log.debug("Job processing vetoed.");
         eventBus.publishEvent(new JobSkipped(this, queue, job, null));
         return;
      }

      Runnable runner = jobRunnerFactory.runnerFor(job);
      if (runner == null) {
         log.error("No job runner found.", name);
         throw new IllegalArgumentException("No job runner found.");
      }

      execute(queue, execution, runner);
   }

   /**
    * Process job.
    *
    * @param queue
    *           Name of queue.
    * @param execution
    *           Job.
    * @param runner
    *           Job runner.
    * @throws Throwable
    *            In case of errors.
    */
   protected void execute(String queue, Execution execution, Runnable runner) throws Throwable {
      Object job = execution.getJob();

      JobExecute jobExecute = new JobExecute(this, queue, job, runner);
      eventBus.publishEvent(jobExecute);
      if (jobExecute.isVeto()) {
         log.debug("Job execution vetoed.");
         eventBus.publishEvent(new JobSkipped(this, queue, job, runner));
         return;
      }

      log.info("Starting job.");
      try {
         runner.run();
         log.info("Job succeeded.");
         workerDao.success(name);
         eventBus.publishEvent(new JobSuccess(this, queue, job, runner));
      } catch (Throwable t) {
         log.info("Job failed.", t);
         workerDao.failure(name);
         eventBus.publishEvent(new JobFailed(this, queue, job, runner));
         throw new IllegalArgumentException("Job failed.", t);
      } finally {
         log.info("Job finished.", name, execution.getId());
      }
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof Worker && id == ((Worker) o).getId();
   }

   @Override
   public int hashCode() {
      return id;
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

   /**
    * Queue dao.
    */
   public QueueDao getQueueDao() {
      return queueDao;
   }

   /**
    * Queue dao.
    */
   public void setQueueDao(QueueDao queueDao) {
      this.queueDao = queueDao;
   }

   /**
    * Worker dao.
    */
   public WorkerDao getWorkerDao() {
      return workerDao;
   }

   /**
    * Worker dao.
    */
   public void setWorkerDao(WorkerDao workerDao) {
      this.workerDao = workerDao;
   }

   /**
    * Factory for creating job runners.
    */
   public JobRunnerFactory getJobRunnerFactory() {
      return jobRunnerFactory;
   }

   /**
    * Factory for creating job runners.
    */
   public void setJobRunnerFactory(JobRunnerFactory jobRunnerFactory) {
      this.jobRunnerFactory = jobRunnerFactory;
   }

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public long getEmptyQueuesSleepMillis() {
      return emptyQueuesSleepMillis;
   }

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public void setEmptyQueuesSleepMillis(long emptyQueuesSleepMillis) {
      this.emptyQueuesSleepMillis = emptyQueuesSleepMillis;
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher eventBus) {
      this.eventBus = eventBus;
   }
}
