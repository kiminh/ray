package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Sink Function
 * @param <T> SINK element type.
 */
@FunctionalInterface
public interface SinkFunction<T> extends Function {
  void sink(T value);
}
