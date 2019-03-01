package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

@FunctionalInterface
public interface ProcessFunction<T> extends Function {
  void process(T value);
}
