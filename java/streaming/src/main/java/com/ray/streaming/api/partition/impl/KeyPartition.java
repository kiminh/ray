package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;
import com.ray.streaming.message.KeyRecord;

/**
 * Key Partition Default Implementation.
 * @param <K> Type of the partition key.
 * @param <T> Type of the input data.
 */
public class KeyPartition<K, T> implements Partition<KeyRecord<K, T>> {

  @Override
  public int[] partition(KeyRecord<K, T> keyRecord, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[Math.abs(keyRecord.getKey().hashCode() % length)];
    return new int[]{taskId};
  }
}
