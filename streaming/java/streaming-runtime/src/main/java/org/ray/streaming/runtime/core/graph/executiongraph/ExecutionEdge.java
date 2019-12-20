package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

import com.google.common.base.MoreObjects;

public class ExecutionEdge implements Serializable {

  private final ExecutionVertex producer;
  private final ExecutionVertex consumer;
  private final String executionEdgeIndex;

  public ExecutionEdge(ExecutionVertex producer, ExecutionVertex consumer) {
    this.producer = producer;
    this.consumer = consumer;
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return producer.getVertexId() + "—" + consumer.getVertexId();
  }

  public ExecutionVertex getProducer() {
    return producer;
  }

  public ExecutionVertex getConsumer() {
    return consumer;
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("producer", producer)
        .add("consumer", consumer)
        .add("executionEdgeIndex", executionEdgeIndex)
        .toString();
  }
}