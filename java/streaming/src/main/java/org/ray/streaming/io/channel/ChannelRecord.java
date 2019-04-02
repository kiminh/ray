package org.ray.streaming.io.channel;

public class ChannelRecord<T> {
  private T value;

  public ChannelRecord(T value) {
    this.value = value;
  }

  public T value() {
    return value;
  }
}