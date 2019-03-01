package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.RayContext;
import com.ray.streaming.api.function.impl.AggregateFunction;
import com.ray.streaming.api.function.impl.ReduceFunction;
import com.ray.streaming.api.partition.impl.KeyPartition;
import com.ray.streaming.operator.StreamOperator;
import com.ray.streaming.operator.impl.ReduceOperator;

/**
 * KeyBy DataStream.
 * @param <K> partition key
 * @param <T> input element
 */
public class KeyDataStream<K, T> extends DataStream<T> {

  public KeyDataStream(RayContext rayContext, StreamOperator streamOperator) {
    super(rayContext, streamOperator);
  }

  public KeyDataStream(DataStream<T> input, StreamOperator streamOperator) {
    super(input, streamOperator);
    this.partition = new KeyPartition();
  }

  public DataStream<T> reduce(ReduceFunction reduceFunction) {
    return new DataStream<>(this, new ReduceOperator(reduceFunction));
  }

  public <A, O> DataStream<O> aggregate(AggregateFunction<T, A, O> aggregateFunction) {
    return new DataStream<>(this, null);
  }

  public KeyDataStream<K, T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
