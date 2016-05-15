package com.s24.redjob.worker;

import com.s24.redjob.queue.Job;
import com.s24.redjob.queue.QueueDao;
import com.s24.redjob.worker.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of {@link Worker}.
 */
public class WorkerImpl implements Worker, Runnable, ApplicationEventPublisherAware {
    /**
     * Log.
     */
    private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);

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
     * Number of milliseconds the worker pauses, if none of the queues contained a job.
     * Defaults to {@value DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
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
        name = ManagementFactory.getRuntimeMXBean().getName() + ":" + id + ":" +
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
            workerDao.start(name);
            eventBus.publishEvent(new WorkerStart(this));
            poll();
        } catch (Throwable t) {
            log.error("Worker {}: Uncaught exception in worker. Worker stopped.", name, t);
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
                log.debug("Worker {}: Thread has been interrupted.", name);
            } catch (Throwable e) {
                log.error("Worker {}: Polling for jobs failed.", name, e);
            }
        }
    }

    /**
     * Poll all queues.
     *
     * @throws Throwable In case of errors.
     */
    protected void pollQueues() throws Throwable {
        for (String queue : queues) {
            eventBus.publishEvent(new WorkerPoll(this, queue));
            Job job = queueDao.pop(queue, name);
            if (job != null) {
                execute(queue, job);
                return;
            }
        }
        Thread.sleep(emptyQueuesSleepMillis);
    }

    /**
     * Execute job.
     *
     * @param queue Name of queue.
     * @param job Job.
     * @throws Throwable In case of errors.
     */
    protected void execute(String queue, Job job) throws Throwable {
        Object payload = job.getPayload();
        if (payload == null) {
            log.error("Worker {}: Job {}: Missing payload.", name, job.getId());
            throw new IllegalArgumentException("Missing payload.");
        }

        JobProcess jobProcess = new JobProcess(this, queue, payload);
        eventBus.publishEvent(jobProcess);
        if (jobProcess.isVeto()) {
            eventBus.publishEvent(new JobSkipped(this, queue, payload, null));
            return;
        }

        Runnable runner = jobRunnerFactory.runnerFor(payload);
        if (runner == null) {
            log.error("Worker {}: Job {}: No runner found.", name, job.getId());
            throw new IllegalArgumentException("No runner found.");
        }
        JobExecute jobExecute = new JobExecute(this, queue, payload, runner);
        eventBus.publishEvent(jobExecute);
        if (jobExecute.isVeto()) {
            eventBus.publishEvent(new JobSkipped(this, queue, payload, runner));
            return;
        }

        log.info("Worker {}: Job {}: Start.", name, job.getId());
        try {
           runner.run();
            log.info("Worker {}: Job {}: Success.", name, job.getId());
            workerDao.success(name);
            eventBus.publishEvent(new JobSuccess(this, queue, payload, runner));
        } catch (Throwable t) {
            log.error("Worker {}: Job {}: Failed to execute.", name, job.getId(), t);
            workerDao.failure(name);
            eventBus.publishEvent(new JobFailed(this, queue, payload, runner));
            throw new IllegalArgumentException("Failed to execute.", t);
        } finally {
            log.info("Worker {}: Job {}: End.", name, job.getId());
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
     * Number of milliseconds the worker pauses, if none of the queues contained a job.
     * Defaults to {@value DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
     */
    public long getEmptyQueuesSleepMillis() {
        return emptyQueuesSleepMillis;
    }

    /**
     * Number of milliseconds the worker pauses, if none of the queues contained a job.
     * Defaults to {@value DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS}.
     */
    public void setEmptyQueuesSleepMillis(long emptyQueuesSleepMillis) {
        this.emptyQueuesSleepMillis = emptyQueuesSleepMillis;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher eventBus) {
        this.eventBus = eventBus;
    }
}
