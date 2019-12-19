package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;

import org.ray.streaming.api.partition.Partition;

public class ExecutionJobEdge {

  private final ExecutionJobVertex producer;
  private final ExecutionJobVertex consumer;
  private final String executionJobEdgeIndex;
  private Partition partition;

  public ExecutionJobEdge(ExecutionJobVertex producer, ExecutionJobVertex consumer, Partition partition) {
    this.producer = producer;
    this.consumer = consumer;
    this.partition = partition;
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return producer.getJobVertexId() + "—" + consumer.getJobVertexId();
  }

  public ExecutionJobVertex getProducer() {
    return producer;
  }

  public ExecutionJobVertex getConsumer() {
    return consumer;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("producer", producer)
        .add("consumer", consumer)
        .add("partition", partition)
        .toString();
  }
}
