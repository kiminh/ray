package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Map Function.
 * @param <T> input element
 * @param <R> output element
 */
@FunctionalInterface
public interface MapFunction<T, R> extends Function {
  R map(T value);
}
