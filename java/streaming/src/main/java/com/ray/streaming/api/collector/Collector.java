package com.ray.streaming.api.collector;

/**
 * collector output and emit to downstream operator.
 * @param <T>
 */
public interface Collector<T> {

  void collect(T value);

}
