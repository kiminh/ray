package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.IPartition;

/**
 * Random Partition Default Implementation.
 * @param <T> input element type
 */
public class RandomPartition<T> implements IPartition<T> {

  private int seq;

  public RandomPartition() {
    this.seq = 0;
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[seq++ % length];
    return new int[]{taskId};
  }
}
