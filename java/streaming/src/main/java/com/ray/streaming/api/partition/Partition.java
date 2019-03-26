package com.ray.streaming.api.partition;

import com.ray.streaming.api.function.Function;

/**
 * partition interface, partition record to downstream task.
 * @param <T> partition input element type
 */
@FunctionalInterface
public interface Partition<T> extends Function {

  int[] partition(T value, int[] taskIds);

}
