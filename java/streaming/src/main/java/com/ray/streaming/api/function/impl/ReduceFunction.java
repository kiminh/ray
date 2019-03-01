package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Reduce Function.
 * @param <T> input element
 */
@FunctionalInterface
public interface ReduceFunction<T> extends Function {
  T reduce(T oldValue, T newValue);
}
