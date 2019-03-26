package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;

/**
 * Broad Partition Default Implementation.
 * @param <T> broad cast element
 */
public class BoardCastPartition<T> implements Partition<T> {

  public BoardCastPartition() {
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    return taskIds;
  }
}
