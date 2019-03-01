package com.ray.streaming.api.stream;

import com.ray.streaming.operator.impl.SinkOperator;

/**
 * Sink of DataStream.
 * @param <T> SINK element
 */
public class StreamSink<T> extends Stream<T> {

  public StreamSink(DataStream<T> input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    this.rayContext.addSink(this);
  }

  public StreamSink<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
