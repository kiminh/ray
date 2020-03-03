package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray object.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;
  private static byte[] emptyBuffer = new byte[LENGTH];
  private Class<?> type = null;

  /**
   * Create an ObjectId from a ByteBuffer.
   */
  public static ObjectId fromByteBuffer(ByteBuffer bb, Class<?> type) {
    return new ObjectId(byteBuffer2Bytes(bb), type);
  }

  public static ObjectId fromByteBuffer(ByteBuffer bb) {
    return new ObjectId(byteBuffer2Bytes(bb));
  }

  /**
   * Generate an ObjectId with random value.
   */
  public static ObjectId fromRandom(Class<?> type) {
    // This is tightly coupled with ObjectID definition in C++. If that changes,
    // this must be changed as well.
    // The following logic should be kept consistent with `ObjectID::FromRandom` in
    // C++.
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    Arrays.fill(b, TaskId.LENGTH, LENGTH, (byte) 0);
    return new ObjectId(b, type);
  }

  public static ObjectId fromRandom() {
    return fromRandom(null);
  }

  public static ObjectId fromType(Class<?> type) {
    return new ObjectId(emptyBuffer, type);
  }

  public ObjectId(byte[] id) {
    super(id);
  }

  public ObjectId(byte[] id, Class<?> type) {
    super(id);
    this.type = type;
  }

  @Override
  public int size() {
    return LENGTH;
  }

  public Class<?> getType() { return type; }

}
