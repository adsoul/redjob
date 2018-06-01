package com.s24.redjob.worker;

import com.s24.redjob.worker.events.JobExecute;
import com.s24.redjob.worker.events.JobFailure;
import com.s24.redjob.worker.events.JobProcess;
import com.s24.redjob.worker.events.JobSkipped;
import com.s24.redjob.worker.events.JobStart;
import com.s24.redjob.worker.events.JobSuccess;
import com.s24.redjob.worker.events.WorkerEvent;
import com.s24.redjob.worker.events.WorkerStopping;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base implementation of {@link Worker}.
 */
public abstract class AbstractWorker<S extends WorkerState> implements Worker, ApplicationEventPublisherAware, DisposableBean {
   /**
    * Log.
    */
   protected final Logger log = LoggerFactory.getLogger(getClass());

   /**
    * Placeholder for the worker id.
    */
   public static final String ID = "[id]";

   /**
    * Sequence for worker ids.
    */
   private static final AtomicInteger IDS = new AtomicInteger();

   /**
    * Worker id.
    */
   private int id;

   /**
    * Name of this worker. Defaults to a generated unique name.
    */
   protected String name;

   /**
    * Worker dao.
    */
   protected WorkerDao workerDao;

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
    * #DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   protected long emptyQueuesSleepMillis = DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS;

   /**
    * Should worker run?.
    */
   protected final AtomicBoolean run = new AtomicBoolean(true);

   /**
    * Worker state.
    */
   protected S state;

   /**
    * Event bus.
    */
   protected ApplicationEventPublisher eventBus;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(jobRunnerFactory, "Precondition violated: jobRunnerFactory != null.");
      Assert.isTrue(emptyQueuesSleepMillis > 0, "Precondition violated: emptyQueuesSleepMillis > 0.");
      Assert.notNull(eventBus, "Precondition violated: eventBus != null.");

      id = IDS.incrementAndGet();
      if (StringUtils.hasLength(name)) {
         name = resolvePlaceholders(name);
      } else {
         name = createName();
      }
   }

   @Override
   public void destroy() {
      // TODO markus 2016-06-15: Interrupt too?
      stop();
   }

   /**
    * Resolve placeholder in custom worker names.
    *
    * Supported placeholders:
    * <ul>
    *    <li>id: Worker id.</li>
    *    <li>hostname: Hostname without domain.</li>
    *    <li>full-hostname: Hostname with domain.</li>
    * </ul>
    */
   protected String resolvePlaceholders(String name) throws Exception {
      name = name.replaceAll(Pattern.quote(ID), Long.toString(id));

      return HostnameResolver.resolve(name);
   }

   /**
    * Create name for this worker.
    */
   protected String createName() {
      return ManagementFactory.getRuntimeMXBean().getName() + ":" + id;
   }

   @Override
   public String getNamespace() {
      return workerDao.getNamespace();
   }

   @Override
   public int getId() {
      return id;
   }

   @Override
   public String getName() {
      return name;
   }

   /**
    * Set worker state to the given value.
    */
   protected void setWorkerState(String state, WorkerEvent event) {
      if (this.state != null) {
         this.state.setState(state);
         saveWorkerState();
      }
      eventBus.publishEvent(event);
   }

   /**
    * Save worker state.
    */
   protected void saveWorkerState() {
      try {
         workerDao.state(name, this.state);
      } catch (Exception e) {
         log.error("Failed to set worker state to {}.", state);
      }
   }

   @Override
   public void stop() {
      if (state != null && !state.isState(WorkerState.STOPPING, WorkerState.STOPPED, WorkerState.FAILED)) {
         log.info("Stopping worker {}.", getName(), new Exception());
         run.set(false);
         setWorkerState(WorkerState.STOPPING, new WorkerStopping(this));
      }
   }

   @Override
   public void waitUntilStopped() {
      if (this.state == null || this.state.isState(WorkerState.STOPPED, WorkerState.FAILED)) {
         return;
      }

      log.info("Waiting for worker {} to stop.", getName());
      while (!this.state.isState(WorkerState.STOPPED, WorkerState.FAILED)) {
         try {
            Thread.sleep(100);
         } catch (InterruptedException e) {
            // Ignore.
         }
      }
   }

   /**
    * Process job. Sends {@link JobProcess} event.
    *
    * @param queue
    *           Name of queue.
    * @param execution
    *           Job.
    * @return true, if job needs to be re-enqueued. false, otherwise.
    * @throws Throwable
    *            In case of errors.
    */
   protected <J> boolean process(String queue, Execution execution) throws Throwable {
      J job = execution.getJob();
      if (job == null) {
         log.error("Missing job.");
         throw new IllegalArgumentException("Missing job.");
      }

      JobProcess jobProcess = new JobProcess(this, queue, execution);
      eventBus.publishEvent(jobProcess);
      if (jobProcess.isVeto()) {
         log.debug("Job processing vetoed.");
         eventBus.publishEvent(new JobSkipped(this, queue, execution, null));
         return true;
      }

      Runnable runner = jobRunnerFactory.runnerFor(job);
      if (runner == null) {
         log.error("No job runner found.", name);
         throw new IllegalArgumentException("No job runner found.");
      }

      execute(queue, execution, runner);
      return false;
   }

   /**
    * Execute job. Sends {@link JobExecute} event.
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
      Object unwrappedRunner = runner;
      if (runner instanceof WrappingRunnable) {
         unwrappedRunner =  ((WrappingRunnable) runner).unwrap();
      }
      prepareRunner(unwrappedRunner);

      JobExecute jobExecute = new JobExecute(this, queue, execution, unwrappedRunner);
      eventBus.publishEvent(jobExecute);
      if (jobExecute.isVeto()) {
         log.debug("Job execution vetoed.");
         eventBus.publishEvent(new JobSkipped(this, queue, execution, unwrappedRunner));
         return;
      }

      try {
         execution.start(getName());
         run(queue, execution, runner, unwrappedRunner);
      } finally {
         execution.stop();
      }
   }

   /**
    * Run job.
    *
    * @param queue
    *           Name of queue.
    * @param execution
    *           Job.
    * @param runner
    *           Job runner.
    */
   protected void run(String queue, Execution execution, Runnable runner, Object unwrappedRunner) {
      log.debug("Starting job.");
      eventBus.publishEvent(new JobStart(this, queue, execution, unwrappedRunner));
      try {
         runner.run();
         log.debug("Job succeeded.");
         state.incSuccess();
         saveWorkerState();
         workerDao.success(name);
         eventBus.publishEvent(new JobSuccess(this, queue, execution, unwrappedRunner));
      } catch (Throwable cause) {
         log.warn("Job failed.", cause);
         state.incFailed();
         saveWorkerState();
         workerDao.failure(name);
         eventBus.publishEvent(new JobFailure(this, queue, execution, unwrappedRunner, cause));
         throw new IllegalArgumentException("Job failed.", cause);
      } finally {
         log.debug("Job finished.", name, execution.getId());
      }
   }

   /**
    * Prepare runner.
    */
   protected void prepareRunner(Object runner) {
      // Overwrite, if needed.
   }

   @Override
   public boolean equals(Object o) {
      return o instanceof Worker && id == ((Worker) o).getId();
   }

   @Override
   public int hashCode() {
      return id;
   }

   @Override
   public String toString() {
      return getName();
   }

   //
   // Injections.
   //

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
    * Name of this worker. Defaults to a generated unique name.
    */
   public void setName(String name) {
      this.name = name;
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
    * #DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public long getEmptyQueuesSleepMillis() {
      return emptyQueuesSleepMillis;
   }

   /**
    * Number of milliseconds the worker pauses, if none of the queues contained a job. Defaults to {@value
    * #DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
    */
   public void setEmptyQueuesSleepMillis(long emptyQueuesSleepMillis) {
      this.emptyQueuesSleepMillis = emptyQueuesSleepMillis;
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher eventBus) {
      this.eventBus = eventBus;
   }
}
