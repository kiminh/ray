package org.ray.streaming.runtime.core.graph.executiongraph;

import com.alipay.streaming.runtime.jobgraph.DistributionPattern;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IntermediateResultPartition implements Serializable {

  private IntermediateResult totalResult;
  private ExecutionVertex producer;
  private int partitionIndex;
  private List<ExecutionEdge> consumers = new ArrayList<>();

  public IntermediateResultPartition(
      IntermediateResult totalResult, ExecutionVertex producer, int partitionIndex,
      DistributionPattern distributionPattern, ExecutionJobVertex consumer) {
    this.totalResult = totalResult;
    this.producer = producer;
    this.partitionIndex = partitionIndex;

    if (distributionPattern == DistributionPattern.ALL_TO_ALL) {
      updateConsumers(consumer);
    } else {
      // TODO: create consumer execution edges
      throw new RuntimeException("Unsupport POINTWISE pattern now.");
    }
  }

  public IntermediateResult getTotalResult() {
    return totalResult;
  }

  public ExecutionVertex getProducer() {
    return producer;
  }

  public int getPartitionIndex() {
    return partitionIndex;
  }

  public List<ExecutionEdge> getConsumers() {
    return consumers;
  }

  public void updateConsumers(ExecutionJobVertex consumer) {
    consumers.clear();
    List<ExecutionVertex> exeVertices = consumer.getExeVertices();
    for (int index = 0; index < exeVertices.size(); ++index) {
      ExecutionEdge executionEdge = new ExecutionEdge(this, exeVertices.get(index), index);
      consumers.add(executionEdge);
    }
  }
}