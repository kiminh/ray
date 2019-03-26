package com.ray.streaming.api.function.impl;

import java.io.Serializable;

/**
 * Interface of Join functions.
 * @param <T> Type of the left input data.
 * @param <O> Type of the right input data.
 * @param <R> Type of the output data.
 */
@FunctionalInterface
public interface JoinFunction<T, O, R> extends Serializable {
  R join(T left, O right);
}
