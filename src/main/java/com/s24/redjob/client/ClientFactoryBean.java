package com.s24.redjob.client;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.s24.redjob.AbstractDao;
import com.s24.redjob.channel.ChannelDaoImpl;
import com.s24.redjob.lock.LockDaoImpl;
import com.s24.redjob.queue.QueueDaoImpl;
import com.s24.redjob.worker.ExecutionRedisSerializer;

/**
 * {@link FactoryBean} for easy creation of a {@link Client}.
 */
public class ClientFactoryBean implements FactoryBean<Client>, InitializingBean {
   /**
    * Queue dao.
    */
   private QueueDaoImpl queueDao = new QueueDaoImpl();

   /**
    * Channel dao.
    */
   private ChannelDaoImpl channelDao = new ChannelDaoImpl();

   /**
    * Lock dao.
    */
   private LockDaoImpl lockDao = new LockDaoImpl();

   /**
    * The instance.
    */
   private final ClientImpl client = new ClientImpl();

   @Override
   public void afterPropertiesSet() throws Exception {
      if (getExecutions() == null) {
         setExecutions(new ExecutionRedisSerializer());
      }

      queueDao.afterPropertiesSet();
      channelDao.afterPropertiesSet();
      lockDao.afterPropertiesSet();

      client.setQueueDao(queueDao);
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
      return queueDao.getConnectionFactory();
   }

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      queueDao.setConnectionFactory(connectionFactory);
      channelDao.setConnectionFactory(connectionFactory);
      lockDao.setConnectionFactory(connectionFactory);
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public String getNamespace() {
      return queueDao.getNamespace();
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value AbstractDao#DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      queueDao.setNamespace(namespace);
      channelDao.setNamespace(namespace);
      lockDao.setNamespace(namespace);
   }

   /**
    * Redis serializer for job executions.
    */
   public ExecutionRedisSerializer getExecutions() {
      return queueDao.getExecutions();
   }

   /**
    * Redis serializer for job executions.
    */
   public void setExecutions(ExecutionRedisSerializer executions) {
      queueDao.setExecutions(executions);
      channelDao.setExecutions(executions);
   }
}
