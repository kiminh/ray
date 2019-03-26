package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.RayContext;
import com.ray.streaming.api.partition.Partition;
import com.ray.streaming.api.partition.impl.RRPartition;
import com.ray.streaming.operator.StreamOperator;
import java.io.Serializable;

/**
 * Represents an abstract stream of data.
 * @param <T>
 */
public class Stream<T> implements Serializable {

  protected int id;
  protected int parallelism = 1;
  protected StreamOperator operator;
  protected Stream<T> inputStream;
  protected RayContext rayContext;
  protected Partition<T> partition;

  public Stream(RayContext rayContext, StreamOperator streamOperator) {
    this.rayContext = rayContext;
    this.operator = streamOperator;
    this.id = rayContext.generateId();
    this.partition = new RRPartition<>();
  }

  public Stream(Stream<T> inputStream, StreamOperator streamOperator) {
    this.inputStream = inputStream;
    this.parallelism = inputStream.getParallelism();
    this.rayContext = this.inputStream.getRayContext();
    this.operator = streamOperator;
    this.id = rayContext.generateId();
    this.partition = new RRPartition<>();
  }


  public Stream<T> getInputStream() {
    return inputStream;
  }

  public StreamOperator getOperator() {
    return operator;
  }

  public RayContext getRayContext() {
    return rayContext;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Stream<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public int getId() {
    return id;
  }

  public Partition<T> getPartition() {
    return partition;
  }

  public void setPartition(Partition<T> partition) {
    this.partition = partition;
  }
}
