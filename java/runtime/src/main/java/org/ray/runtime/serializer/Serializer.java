package org.ray.runtime.serializer;

public class Serializer {
  public static void setClassloader(ClassLoader classLoader) {
    MessagePackSerializer.setClassloader(classLoader);
  }

  public static byte[] encode(Object obj) {
    return MessagePackSerializer.encode(obj, null);
  }

  public static byte[] encode(Object obj, ClassLoader classLoader) {
    return MessagePackSerializer.encode(obj, classLoader);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type) {
    return MessagePackSerializer.decode(bs, type, null);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type, ClassLoader classLoader) {
    return MessagePackSerializer.decode(bs, type, classLoader);
  }
}