package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.RayContext;
import com.ray.streaming.api.function.internal.CollectionSourceFunction;
import com.ray.streaming.operator.impl.SourceOperator;
import java.util.List;

/**
 * Source of DataStream.
 * @param <T> SOURCE collect element
 */
public class StreamSource<T> extends DataStream<T> {

  public StreamSource(RayContext rayContext, SourceOperator sourceOperator) {
    super(rayContext, sourceOperator);
  }

  public static <T> StreamSource<T> buildSource(RayContext context, List<T> values) {
    return new StreamSource(context, new SourceOperator(new CollectionSourceFunction(values)));
  }

  public StreamSource<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
