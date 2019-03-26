package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.StreamingContext;
import com.ray.streaming.api.function.impl.AggregateFunction;
import com.ray.streaming.api.function.impl.ReduceFunction;
import com.ray.streaming.api.partition.impl.KeyPartition;
import com.ray.streaming.operator.StreamOperator;
import com.ray.streaming.operator.impl.ReduceOperator;

/**
 * Represents an DataStream of partition by key.
 * @param <K> The type of Key-By data.
 * @param <T> The type of KeyDataStream data.
 */
public class KeyDataStream<K, T> extends DataStream<T> {

  public KeyDataStream(StreamingContext streamingContext, StreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public KeyDataStream(DataStream<T> input, StreamOperator streamOperator) {
    super(input, streamOperator);
    this.partition = new KeyPartition();
  }

  /**
   * Apply Reduce Function on a DataStream.
   * @param reduceFunction the reduce function
   * @return A new DataStream
   */
  public DataStream<T> reduce(ReduceFunction reduceFunction) {
    return new DataStream<>(this, new ReduceOperator(reduceFunction));
  }

  /**
   * Apply Aggregate Function on a DataStream
   * @param aggregateFunction the aggregate function
   * @param <A> The type of aggregate intermediate data.
   * @param <O> The type of result data.
   * @return A new DataStream.
   */
  public <A, O> DataStream<O> aggregate(AggregateFunction<T, A, O> aggregateFunction) {
    return new DataStream<>(this, null);
  }

  /**
   * Set parallelism to current transformation
   * @param parallelism the parallelism of this transformation
   * @return A new DataStream
   */
  public KeyDataStream<K, T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
