package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Interface of KeyBy functions.
 * @param <T> type of the input data.
 * @param <K> type of the keyBy field.
 */
@FunctionalInterface
public interface KeyFunction<T, K> extends Function {
  K keyBy(T value);
}
