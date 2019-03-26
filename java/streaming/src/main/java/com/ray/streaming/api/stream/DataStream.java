package com.ray.streaming.api.stream;


import com.ray.streaming.api.context.StreamingContext;
import com.ray.streaming.api.function.impl.FlatMapFunction;
import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.api.function.impl.MapFunction;
import com.ray.streaming.api.function.impl.SinkFunction;
import com.ray.streaming.api.partition.Partition;
import com.ray.streaming.api.partition.impl.BroadcastPartition;
import com.ray.streaming.operator.StreamOperator;
import com.ray.streaming.operator.impl.FlatMapOperator;
import com.ray.streaming.operator.impl.KeyByOperator;
import com.ray.streaming.operator.impl.MapOperator;
import com.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents an stream of data and contains operation on DataStream.
 * @param <T> Type of DataStream data.
 */
public class DataStream<T> extends Stream<T> {

  public DataStream(StreamingContext streamingContext, StreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public DataStream(DataStream input, StreamOperator streamOperator) {
    super(input, streamOperator);
  }

  /**
   * Apply a map function on the stream.
   * @param mapFunction The map function.
   * @param <R> Type of data returned by the map function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> map(MapFunction<T, R> mapFunction) {
    return new DataStream<>(this, new MapOperator(mapFunction));
  }

  /**
   * Apply a flat-map function on the stream.
   * @param flatMapFunction The FlatMapFunction
   * @param <R> Type of data returned by the flatmap function.
   * @return A new DataStream
   */
  public <R> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
    return new DataStream(this, new FlatMapOperator(flatMapFunction));
  }

  /**
   * Apply a union transform on the stream with another stream.
   * @param other Another stream.
   * @return A new UnionStream.
   */
  public UnionStream<T> union(DataStream<T> other) {
    return new UnionStream(this, null, other);
  }

  /**
   * Apply a join transform on the stream with another stream.
   * @param other Another stream.
   * @param <O> The type of the other stream data.
   * @param <R> The type of the data in the joined stream.
   * @return A new JoinStream.
   */
  public <O, R> JoinStream<T, O, R> join(DataStream<O> other) {
    return new JoinStream<>(this, other);
  }

  // TODO(zhenxuanpan): Need to add processFunction.
  public <R> DataStream<R> process() {
    return new DataStream(this, null);
  }

  /**
   * Apply a sink function and get a StreamSink
   * @param sinkFunction the sink function
   * @return A StreamSink
   */
  public StreamSink<T> sink(SinkFunction<T> sinkFunction) {
    return new StreamSink<>(this, new SinkOperator(sinkFunction));
  }

  /**
   * Apply a key-by function on a DataStream.
   * @param keyFunction the key function.
   * @param <K> The type of the key.
   * @return A new KeyDataStream.
   */
  public <K> KeyDataStream<K, T> keyBy(KeyFunction<T, K> keyFunction) {
    return new KeyDataStream<>(this, new KeyByOperator(keyFunction));
  }

  /**
   * Apply broadcast to a DataStream
   * @return A new DataStream
   */
  public DataStream<T> broadcast() {
    this.partition = new BroadcastPartition<>();
    return this;
  }

  /**
   * Apply a partition to a DataStream
   * @param partition shuffle strategy
   * @return A new DataStream
   */
  public DataStream<T> partitionBy(Partition<T> partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Set parallelism to current transformation
   * @param parallelism the parallelism of this transformation
   * @return A new DataStream
   */
  public DataStream<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }


}
