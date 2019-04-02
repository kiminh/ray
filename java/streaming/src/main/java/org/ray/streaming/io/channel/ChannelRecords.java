package org.ray.streaming.io.channel;

import java.util.List;

public class ChannelRecords<T> {
  private List<ChannelRecord<T>> records;

  public int count() {
    return records.size();
  }

  public boolean isEmpty() {
    return records.isEmpty();
  }

  public List<ChannelRecord<T>> records() {
    return records;
  }
}