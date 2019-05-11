package com.adsoul.redjob;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.lang.Nullable;

/**
 * Redis serializer simply passing the byte[] through.
 */
public class ByteArrayRedisSerializer implements RedisSerializer<byte[]> {
   @Nullable
   @Override
   public byte[] serialize(@Nullable byte[] bytes) throws SerializationException {
      return bytes;
   }

   @Nullable
   @Override
   public byte[] deserialize(@Nullable byte[] bytes) throws SerializationException {
      return bytes;
   }
}
