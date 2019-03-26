package com.ray.streaming.api.partition;

import com.ray.streaming.api.function.Function;

/**
 * Interface of the shuffling strategy.
 *
 * @param <T> partition input element type
 */
@FunctionalInterface
public interface Partition<T> extends Function {

  /**
   * Determine which task(s) should receive the given data.
   * @param value The data.
   * @param taskIds IDs of all downstream operators.
   * @return IDs of the downstream operators that should receive the data.
   */
  int[] partition(T value, int[] taskIds);

}
