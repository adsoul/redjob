package com.s24.redjob;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * Base dao.
 */
public class AbstractDao {
   /**
    * {@link RedisConnectionFactory} to access Redis.
    */
   protected RedisConnectionFactory connectionFactory;

   /**
    * Redis serializer for strings.
    */
   protected final StringRedisSerializer strings = new StringRedisSerializer();

   /**
    * Default namespace.
    */
   public static final String DEFAULT_NAMESPACE = "redjob";

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
   public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
   }

   /**
    * Redis "namespace" to use. Prefix for all Redis keys. Defaults to {@value #DEFAULT_NAMESPACE}.
    */
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
