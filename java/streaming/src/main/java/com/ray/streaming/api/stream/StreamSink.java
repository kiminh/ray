package com.ray.streaming.api.stream;

import com.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents an sink of DataStream
 * @param <T> SINK element
 */
public class StreamSink<T> extends Stream<T> {

  public StreamSink(DataStream<T> input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    this.streamingContext.addSink(this);
  }

  /**
   * Set parallelism to current transformation
   * @param parallelism the parallelism of this transformation
   * @return A new DataStream
   */
  public StreamSink<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
