package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;

/**
 * Round Robin partition record to downstream Tasks.
 * @param <T> Type of the input data.
 */
public class RoundRobinPartition<T> implements Partition<T> {

  private int seq;

  public RoundRobinPartition() {
    this.seq = 0;
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[seq++ % length];
    return new int[]{taskId};
  }
}
