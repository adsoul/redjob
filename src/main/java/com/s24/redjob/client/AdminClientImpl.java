package com.s24.redjob.client;

import com.s24.redjob.queue.QueueDao;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;

/**
 * Default implementation of {@link AdminClient}.
 */
public class AdminClientImpl implements AdminClient {
    /**
     * Queue dao.
     */
    private QueueDao queueDao;

    /**
     * Init.
     */
    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(queueDao, "Precondition violated: queueDao != null.");
    }

    @Override
    public void publish(String channel, Object payload) {

    }

    //
    // Injections.
    //

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
}
