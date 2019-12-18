package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.ray.streaming.runtime.core.graph.jobgraph.DistributionPattern;

public class IntermediateResult implements Serializable {

  private ExecutionJobVertex producer;
  private ExecutionJobVertex consumer;
  private int producerParallelism;
  private int consumerParallelism;
  private List<IntermediateResultPartition> partitions;
  private DistributionPattern distributionPattern;

  public IntermediateResult(ExecutionJobVertex producer, ExecutionJobVertex consumer,
      DistributionPattern distributionPattern) {
    this.producer = producer;
    this.consumer = consumer;
    this.producerParallelism = producer.getParallelism();
    this.consumerParallelism = consumer.getParallelism();
    this.distributionPattern = distributionPattern;
    this.partitions = createPartitions();
  }

  public ExecutionJobVertex getProducer() {
    return producer;
  }

  public ExecutionJobVertex getConsumer() {
    return consumer;
  }

  public int getProducerParallelism() {
    return producerParallelism;
  }

  public int getConsumerParallelism() {
    return consumerParallelism;
  }

  public List<IntermediateResultPartition> getPartitions() {
    return partitions;
  }

  private List<IntermediateResultPartition> createPartitions() {
    List<IntermediateResultPartition> partitions = new ArrayList<>();
    for (int index = 0; index < producerParallelism; ++index) {
      IntermediateResultPartition partition = new IntermediateResultPartition(producer.getExecutionVertices().get(index), consumer, distributionPattern);
      partitions.add(partition);
    }
    return partitions;
  }
}