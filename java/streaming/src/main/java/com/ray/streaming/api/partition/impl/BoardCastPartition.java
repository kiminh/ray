package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.IPartition;

/**
 * Broad Partition Default Implementation.
 * @param <T> broad cast element
 */
public class BoardCastPartition<T> implements IPartition<T> {

  public BoardCastPartition() {
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    return taskIds;
  }
}
