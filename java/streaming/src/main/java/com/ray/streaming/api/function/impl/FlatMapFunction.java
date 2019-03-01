package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.api.function.Function;

@FunctionalInterface
public interface FlatMapFunction<T, R> extends Function {
  void flatMap(T value, Collector<R> collector);
}
