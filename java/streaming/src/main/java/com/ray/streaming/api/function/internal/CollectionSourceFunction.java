package com.ray.streaming.api.function.internal;

import com.ray.streaming.api.function.impl.SourceFunction;
import java.util.List;

/**
 * Collection Source Function.
 * @param <T> output element
 */
public class CollectionSourceFunction<T> implements SourceFunction<T> {

  private List<T> values;

  public CollectionSourceFunction(List<T> values) {
    this.values = values;
  }

  @Override
  public void init(int parallel, int index) {
  }

  @Override
  public void fetch(long batchId, SourceContext<T> ctx) throws Exception {
    for (T value : values) {
      ctx.collect(value);
    }
  }

  @Override
  public void close() {
  }

}
