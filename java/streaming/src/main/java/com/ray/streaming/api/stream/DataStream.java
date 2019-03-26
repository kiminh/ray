package com.ray.streaming.api.stream;


import com.ray.streaming.api.context.RayContext;
import com.ray.streaming.api.function.impl.FlatMapFunction;
import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.api.function.impl.MapFunction;
import com.ray.streaming.api.function.impl.SinkFunction;
import com.ray.streaming.api.partition.Partition;
import com.ray.streaming.api.partition.impl.BoardCastPartition;
import com.ray.streaming.operator.StreamOperator;
import com.ray.streaming.operator.impl.FlatMapOperator;
import com.ray.streaming.operator.impl.KeyByOperator;
import com.ray.streaming.operator.impl.MapOperator;
import com.ray.streaming.operator.impl.SinkOperator;

/**
 * Abstract for DataStream.
 * @param <T> DataStream PROCESS element type
 */
public class DataStream<T> extends Stream<T> {

  public DataStream(RayContext rayContext, StreamOperator streamOperator) {
    super(rayContext, streamOperator);
  }

  public DataStream(DataStream input, StreamOperator streamOperator) {
    super(input, streamOperator);
  }

  public <R> DataStream<R> map(MapFunction<T, R> mapFunction) {
    return new DataStream<>(this, new MapOperator(mapFunction));
  }

  public <R> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
    return new DataStream(this, new FlatMapOperator(flatMapFunction));
  }

  public UnionStream<T> union(DataStream<T> other) {
    return new UnionStream(this, null, other);
  }

  public <O, R> JoinStream<T, O, R> join(DataStream<O> other) {
    return new JoinStream<>(this, other);
  }

  public <R> DataStream<R> process() {
    return new DataStream(this, null);
  }

  public StreamSink<T> sink(SinkFunction<T> sinkFunction) {
    return new StreamSink<>(this, new SinkOperator(sinkFunction));
  }

  //=======================shuffle======================================
  public <K> KeyDataStream<K, T> keyBy(KeyFunction<T, K> keyFunction) {
    return new KeyDataStream<>(this, new KeyByOperator(keyFunction));
  }

  public DataStream<T> broadcast() {
    this.partition = new BoardCastPartition<>();
    return this;
  }

  public DataStream<T> partitionBy(Partition<T> partition) {
    this.partition = partition;
    return this;
  }

  //=======================config======================================
  public DataStream<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }


}
