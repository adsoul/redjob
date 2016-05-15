package com.s24.redjob.worker;

import com.google.common.eventbus.EventBus;
import com.s24.redjob.queue.Job;
import com.s24.redjob.queue.QueueDao;
import com.s24.redjob.worker.events.WorkerError;
import com.s24.redjob.worker.events.WorkerPoll;
import com.s24.redjob.worker.events.WorkerStart;
import com.s24.redjob.worker.events.WorkerStopped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class WorkerImpl implements Runnable, Worker {
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
    private EventBus eventBus = new EventBus();

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
            workerDao.start(this);
            eventBus.post(new WorkerStart(this));
            poll();
        } catch (Throwable t) {
            log.error("Worker {}: Uncaught exception in worker. Worker stopped.", name, t);
            eventBus.post(new WorkerError(this, t));
        } finally {
            eventBus.post(new WorkerStopped(this));
            workerDao.stop(this);
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
            eventBus.post(new WorkerPoll(this, queue));
            Job job = queueDao.pop(queue, name);
            if (job != null) {
                execute(job);
                return;
            }
        }
        Thread.sleep(emptyQueuesSleepMillis);
    }

    /**
     * Execute job.
     *
     * @param job Job.
     * @throws Throwable In case of errors.
     */
    protected void execute(Job job) throws Throwable {
        if (job.getPayload() == null) {
            log.error("Worker {}: Job {}: Missing payload.", name, job.getId());
            throw new IllegalArgumentException("Missing payload.");
        }

        Runnable runner = jobRunnerFactory.runnerFor(job.getPayload());
        if (runner == null) {
            log.error("Worker {}: Job {}: No runner found.", name, job.getId());
            throw new IllegalArgumentException("No runner found.");
        }

        log.info("Worker {}: Job {}: Start.", name, job.getId());
        try {
           runner.run();
        } catch (Throwable t) {
            log.error("Worker {}: Job {}: Failed to execute.", name, job.getId(), t);
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

    /**
     * Event bus.
     */
    public EventBus getEventBus() {
        return eventBus;
    }

    /**
     * Event bus.
     */
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }
}
