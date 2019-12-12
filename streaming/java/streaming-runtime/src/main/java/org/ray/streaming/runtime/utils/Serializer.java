package org.ray.streaming.runtime.utils;

/**
 *
 */
public class Serializer {

  public static byte[] encode(Object obj) {
    return KryoUtils.writeToByteArray(obj);
  }

  public static <T> T decode(byte[] bytes) {
    return KryoUtils.readFromByteArray(bytes);
  }
}
