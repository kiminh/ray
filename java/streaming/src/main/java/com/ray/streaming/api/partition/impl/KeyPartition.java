package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;
import com.ray.streaming.message.KeyRecord;

/**
 * Key Partition Default Implementation.
 * @param <K> key partition element type
 * @param <T> input element type
 */
public class KeyPartition<K, T> implements Partition<KeyRecord<K, T>> {

  @Override
  public int[] partition(KeyRecord<K, T> keyRecord, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[Math.abs(keyRecord.getKey().hashCode() % length)];
    return new int[]{taskId};
  }
}
