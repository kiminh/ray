package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.ray.streaming.runtime.core.graph.jobgraph.DistributionPattern;

public class IntermediateResultPartition implements Serializable {

  private ExecutionVertex producer;
  private List<ExecutionEdge> consumers = new ArrayList<>();

  public IntermediateResultPartition(ExecutionVertex producer, ExecutionJobVertex consumer,
      DistributionPattern distributionPattern) {
    this.producer = producer;

    if (distributionPattern == DistributionPattern.ALL_TO_ALL) {
      updateConsumers(consumer);
    } else {
      // TODO: create consumer execution edges
      throw new RuntimeException("Unsupport POINTWISE pattern now.");
    }
  }

  public ExecutionVertex getProducer() {
    return producer;
  }

  public List<ExecutionEdge> getConsumers() {
    return consumers;
  }

  public void updateConsumers(ExecutionJobVertex consumer) {
    consumers.clear();
    List<ExecutionVertex> executionVertices = consumer.getExecutionVertices();
    for (int index = 0; index < executionVertices.size(); ++index) {
      ExecutionEdge executionEdge = new ExecutionEdge(this, executionVertices.get(index), index);
      consumers.add(executionEdge);
    }
  }
}