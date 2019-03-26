package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;

/**
 * Interface of Source functions.
 * @param <T> Type of the source output data.
 */
public interface SourceFunction<T> extends Function {

  void init(int parallel, int index);

  void fetch(long batchId, SourceContext<T> ctx) throws Exception;

  void close();

  interface SourceContext<T> {

    void collect(T element) throws Exception;

  }
}
