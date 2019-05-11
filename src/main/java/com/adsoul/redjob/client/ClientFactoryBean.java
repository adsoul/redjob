package com.adsoul.redjob.client;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.adsoul.redjob.AbstractDao;
import com.adsoul.redjob.RedJobRedisConnectionFactory;
import com.adsoul.redjob.channel.ChannelDaoImpl;
import com.adsoul.redjob.lock.LockDaoImpl;
import com.adsoul.redjob.queue.FifoDaoImpl;
import com.adsoul.redjob.worker.WorkerDaoImpl;
import com.adsoul.redjob.worker.json.ExecutionRedisSerializer;

/**
 * {@link FactoryBean} for easy creation of a {@link Client}.
 */
public class ClientFactoryBean implements FactoryBean<Client>, InitializingBean {
   /**
    * Worker dao.
    */
   private final WorkerDaoImpl workerDao = new WorkerDaoImpl();

   /**
    * Queue dao.
    */
   private final FifoDaoImpl fifoDao = new FifoDaoImpl();

   /**
    * Channel dao.
    */
   private final ChannelDaoImpl channelDao = new ChannelDaoImpl();

   /**
    * Lock dao.
    */
   private final LockDaoImpl lockDao = new LockDaoImpl();

   /**
    * The instance.
    */
   private final ClientImpl client = new ClientImpl();

   @Override
   public void afterPropertiesSet() throws Exception {
      workerDao.afterPropertiesSet();
      fifoDao.afterPropertiesSet();
      channelDao.afterPropertiesSet();
      lockDao.afterPropertiesSet();

      client.setWorkerDao(workerDao);
      client.setFifoDao(fifoDao);
      client.setChannelDao(channelDao);
      client.setLockDao(lockDao);
      client.afterPropertiesSet();
   }

   @Override
   public boolean isSingleton() {
      return true;
   }

   @Override
   public Class<Client> getObjectType() {
      return Client.class;
   }

   @Override
   public Client getObject() throws Exception {
      return client;
   }

   //
   // Injections.
   //

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public RedisConnectionFactory getConnectionFactory() {
      return fifoDao.getConnectionFactory();
   }

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   @RedJobRedisConnectionFactory
   @Autowired
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      workerDao.setConnectionFactory(connectionFactory);
      fifoDao.setConnectionFactory(connectionFactory);
      channelDao.setConnectionFactory(connectionFactory);
      lockDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public String getNamespace() {
      return fifoDao.getNamespace();
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      workerDao.setNamespace(namespace);
      fifoDao.setNamespace(namespace);
      channelDao.setNamespace(namespace);
      lockDao.setNamespace(namespace);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return fifoDao.getExecutions();
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      fifoDao.setExecutions(executions);
      channelDao.setExecutions(executions);
   }
}
