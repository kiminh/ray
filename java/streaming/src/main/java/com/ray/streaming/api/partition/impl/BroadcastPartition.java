package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;

/**
 * Shuffle data based on broadcast strategy.
 * @param <T> broad cast element
 */
public class BroadcastPartition<T> implements Partition<T> {

  public BroadcastPartition() {
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    return taskIds;
  }
}
