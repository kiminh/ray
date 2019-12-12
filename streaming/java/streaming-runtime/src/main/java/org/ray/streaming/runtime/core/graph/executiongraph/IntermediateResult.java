package org.ray.streaming.runtime.core.graph.executiongraph;

import com.alipay.streaming.runtime.jobgraph.DistributionPattern;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IntermediateResult implements Serializable {

  private ExecutionJobVertex producer;
  private ExecutionJobVertex consumer;
  private int numParallelProducers;
  private int numConsumers;
  private List<IntermediateResultPartition> partitions;
  private DistributionPattern distributionPattern;

  public IntermediateResult(ExecutionJobVertex producer, ExecutionJobVertex consumer,
      DistributionPattern distributionPattern) {
    this.producer = producer;
    this.consumer = consumer;
    this.numParallelProducers = producer.getParallelism();
    this.numConsumers = consumer.getParallelism();
    this.distributionPattern = distributionPattern;
    this.partitions = createPartitions();
  }

  public ExecutionJobVertex getProducer() {
    return producer;
  }

  public ExecutionJobVertex getConsumer() {
    return consumer;
  }

  public int getNumParallelProducers() {
    return numParallelProducers;
  }

  public int getNumConsumers() {
    return numConsumers;
  }

  public List<IntermediateResultPartition> getPartitions() {
    return partitions;
  }

  private List<IntermediateResultPartition> createPartitions() {
    List<IntermediateResultPartition> partitions = new ArrayList<>();
    for (int index = 0; index < numParallelProducers; ++index) {
      IntermediateResultPartition partition = new IntermediateResultPartition(this,
          producer.getExeVertices().get(index), index, distributionPattern, consumer);
      partitions.add(partition);
    }
    return partitions;
  }

  //----------------------------------------------------------------------------------------------
  // Dynamic update
  //----------------------------------------------------------------------------------------------

  public void updateConsumers(ExecutionJobVertex consumer) {
    this.consumer = consumer;
    this.numConsumers = consumer.getParallelism();
    partitions.forEach(p -> p.updateConsumers(consumer));

    // mark producer'state as TO_UPDATE when rescaling consumer
    producer.getExeVertices().forEach(v -> v.setState(ExecutionVertexState.TO_UPDATE));

    producer.markAsAffectedUpStream();
  }

  public void updateProducers(ExecutionJobVertex producer) {
    this.producer = producer;
    this.numParallelProducers = producer.getParallelism();
    this.partitions = createPartitions();

    // mark consumer'state as TO_UPDATE when rescaling producer
    consumer.getExeVertices().forEach(v -> v.setState(ExecutionVertexState.TO_UPDATE));

    consumer.markAsAffectedDownStream();
  }

}