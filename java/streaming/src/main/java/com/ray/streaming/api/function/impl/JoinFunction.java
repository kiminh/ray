package com.ray.streaming.api.function.impl;

import java.io.Serializable;

/**
 * join function.
 * @param <T> left stream element
 * @param <O> right stream element
 * @param <R> join result element
 */
@FunctionalInterface
public interface JoinFunction<T, O, R> extends Serializable {
  R join(T left, O right);
}
