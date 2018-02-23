package com.s24.redjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Base dao.
 */
public abstract class AbstractDao implements Dao {
   /**
    * Logger.
    */
   protected final Logger log = LoggerFactory.getLogger(getClass());

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   protected RedisConnectionFactory connectionFactory;

   /**
    * Redis serializer for strings.
    */
   protected final StringRedisSerializer strings = new StringRedisSerializer();

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
    */
   protected String namespace = DEFAULT_NAMESPACE;

   /**
    * Init.
    */
   @PostConstruct
   public void afterPropertiesSet() {
      Assert.notNull(connectionFactory, "Precondition violated: connectionFactory != null.");
      Assert.hasLength(namespace, "Precondition violated: namespace has length.");
   }

   //
   // Serialization.
   //

   /**
    * List of multiple keys.
    */
   protected List<byte[]> keys(byte[]... keys) {
      return asList(keys);
   }

   /**
    * Construct Redis key name. Created by joining the namespace and the parts together with ':'.
    *
    * @param parts
    *           Parts of the key name.
    */
   protected byte[] key(String... parts) {
      Assert.notEmpty(parts, "Precondition violated: parts are not empty.");
      return strings.serialize(keyString(parts));
   }

   /**
    * Construct Redis key name. Created by joining the namespace and the parts together with ':'.
    *
    * @param parts
    *           Parts of the key name.
    */
   protected String keyString(String... parts) {
      Assert.notEmpty(parts, "Precondition violated: parts are not empty.");
      return Arrays.stream(parts).collect(Collectors.joining(":", namespace + ":", ""));
   }

   /**
    * Serialize long value.
    *
    * @param value
    *           Long.
    * @return Serialized long.
    */
   protected byte[] value(long value) {
      return value(Long.toString(value));
   }

   /**
    * Serialize string value.
    *
    * @param value
    *           String.
    * @return Serialized string.
    */
   protected byte[] value(String value) {
      return strings.serialize(value);
   }

   //
   // Deserialization.
   //

   /**
    * Deserialize long value.
    *
    * @param valueBytes
    *           Long.
    * @return Deserialized long.
    */
   protected Long parseLong(byte[] valueBytes) {
      return Long.valueOf(parseString(valueBytes));
   }

   /**
    * Deserialize string value.
    *
    * @param valueBytes
    *           Long.
    * @return Deserialized string.
    */
   protected String parseString(byte[] valueBytes) {
      return strings.deserialize(valueBytes);
   }

   //
   // Injections.
   //

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   public RedisConnectionFactory getConnectionFactory() {
      return connectionFactory;
   }

   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   @RedJobRedisConnectionFactory
   @Autowired
   public void setConnectionFactory( RedisConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
    */
   @Override
   public String getNamespace() {
      return namespace;
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
    */
   public void setNamespace(String namespace) {
      this.namespace = namespace;
   }
}
