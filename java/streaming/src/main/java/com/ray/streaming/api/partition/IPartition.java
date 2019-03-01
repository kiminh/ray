package com.ray.streaming.api.partition;

import com.ray.streaming.api.function.Function;

/**
 * partition interface.
 * @param <T> partition input element type
 */
@FunctionalInterface
public interface IPartition<T> extends Function {

  int[] partition(T value, int[] taskIds);

}
