package org.ray.streaming.io.channel;

import java.util.ArrayList;
import java.util.List;

public class ChannelRecords<T> {
  private List<ChannelRecord<T>> records;

  public ChannelRecords() {
    this.records = new ArrayList<>();
  }

  public ChannelRecords(List<ChannelRecord<T>> records) {
    this.records = records;
  }

  public void add(ChannelRecord<T> records) {
    this.records.add(records);
  }

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