package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Key By Function.
 * @param <T> input element type
 * @param <K> key partition type
 */
@FunctionalInterface
public interface KeyFunction<T, K> extends Function {
  K keyBy(T value);
}
