package com.adsoul.redjob.worker;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.adsoul.redjob.worker.events.JobExecute;
import com.adsoul.redjob.worker.events.JobFailure;
import com.adsoul.redjob.worker.events.JobProcess;
import com.adsoul.redjob.worker.events.JobSkipped;
import com.adsoul.redjob.worker.events.JobStart;
import com.adsoul.redjob.worker.events.JobSuccess;
import com.adsoul.redjob.worker.events.WorkerEvent;
import com.adsoul.redjob.worker.events.WorkerStopping;
import com.adsoul.redjob.worker.execution.ExecutionStrategy;

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
    * Execution strategy.
    */
   private ExecutionStrategy executionStrategy;

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
   protected final S state;

   /**
    * Event bus.
    */
   protected ApplicationEventPublisher eventBus;

   /**
    * Constructor.
    *
    * @param state
    *       Initial state.
    */
   public AbstractWorker(S state) {
      Assert.notNull(state, "Pre-condition violated: state != null.");

      this.state = state;
   }

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() throws Exception {
      Assert.notNull(executionStrategy, "Precondition violated: executionStrategy != null.");
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
    * <p>
    * Supported placeholders:
    * <ul>
    * <li>id: Worker id.</li>
    * <li>hostname: Hostname without domain.</li>
    * <li>full-hostname: Hostname with domain.</li>
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
   protected void setWorkerState(Consumer<WorkerState> change, WorkerEvent event) {
      change.accept(this.state);
      saveWorkerState();
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
      if (!state.isStopping() && !state.isTerminated()) {
         log.debug("Stopping worker {}.", getName());
         run.set(false);
         setWorkerState(WorkerState::stop, new WorkerStopping(this));
      }
   }

   @Override
   public void waitUntilStopped() {
      if (state.isTerminated()) {
         return;
      }

      log.info("Waiting for worker {} to stop.", getName());
      while (!state.isTerminated()) {
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
    *       Name of queue.
    * @param execution
    *       Job.
    * @return true, if job needs to be re-enqueued. false, otherwise.
    * @throws Throwable
    *       In case of errors.
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
         eventBus.publishEvent(new JobSkipped(this, queue, execution));
         return true;
      }

      execute(queue, execution);
      return false;
   }

   /**
    * Execute job. Sends {@link JobExecute} event.
    *
    * @param queue
    *       Name of queue.
    * @param execution
    *       Job.
    * @throws Throwable
    *       In case of errors.
    */
   protected void execute(String queue, Execution execution) throws Throwable {
      JobExecute jobExecute = new JobExecute(this, queue, execution);
      eventBus.publishEvent(jobExecute);
      if (jobExecute.isVeto()) {
         log.debug("Job execution vetoed.");
         eventBus.publishEvent(new JobSkipped(this, queue, execution));
         return;
      }

      try {
         execution.start(getName());
         run(queue, execution);
      } finally {
         execution.stop();
      }
   }

   /**
    * Run job.
    *
    * @param queue
    *       Name of queue.
    * @param execution
    *       Job.
    */
   protected void run(String queue, Execution execution) {
      log.debug("Starting job.");
      eventBus.publishEvent(new JobStart(this, queue, execution));
      try {
         executionStrategy.execute(queue, execution);
         log.debug("Job succeeded.");
         state.incSuccess();
         saveWorkerState();
         workerDao.success(name);
         eventBus.publishEvent(new JobSuccess(this, queue, execution));
      } catch (Throwable cause) {
         log.warn("Job failed.", cause);
         state.incFailed();
         saveWorkerState();
         workerDao.failure(name);
         eventBus.publishEvent(new JobFailure(this, queue, execution, cause));
         throw new IllegalArgumentException("Job failed.", cause);
      } finally {
         log.debug("Job finished.", name, execution.getId());
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
    * Execution strategy.
    */
   public ExecutionStrategy getExecutionStrategy() {
      return executionStrategy;
   }

   /**
    * Execution strategy.
    */
   public void setExecutionStrategy(ExecutionStrategy executionStrategy) {
      this.executionStrategy = executionStrategy;
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
