package com.ray.streaming.api.partition.impl;

import com.ray.streaming.api.partition.Partition;

/**
 * Round Robin Partition Record to downstream Tasks.
 * @param <T> input element type
 */
public class RRPartition<T> implements Partition<T> {

  private int seq;

  public RRPartition() {
    this.seq = 0;
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[seq++ % length];
    return new int[]{taskId};
  }
}
