package org.ray.streaming.runtime.core.graph.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IntermediateDataSet implements Serializable {
  private static final long serialVersionUID = 1L;

  private JobVertex producer;
  private final List<JobEdge> consumers = new ArrayList<>();
  private final IPartitioner partitioner;

  public IntermediateDataSet(IPartitioner partitioner, JobVertex producer) {
    this.partitioner = partitioner;
    this.producer = producer;
  }

  public void addConsumer(JobEdge edge) {
    this.consumers.add(edge);
  }

  public JobVertex getProducer() {
    return producer;
  }

  public List<JobEdge> getConsumers() {
    return consumers;
  }

  public IPartitioner getPartitioner() {
    return partitioner;
  }
}