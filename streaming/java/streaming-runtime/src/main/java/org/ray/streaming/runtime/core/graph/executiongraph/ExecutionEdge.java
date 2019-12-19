package org.ray.streaming.runtime.core.graph.executiongraph;

import org.ray.streaming.api.partition.Partition;

/**
 *
 */
public class ExecutionEdge {

  private int srcVertexId;
  private int targetVertexId;
  private Partition partition;

  public ExecutionEdge(int srcVertexId, int targetVertexId, Partition partition) {
    this.srcVertexId = srcVertexId;
    this.targetVertexId = targetVertexId;
    this.partition = partition;
  }

  public int getSrcVertexId() {
    return srcVertexId;
  }

  public int getTargetVertexId() {
    return targetVertexId;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return "ExecutionEdge{" +
        "srcVertexId=" + srcVertexId +
        ", targetVertexId=" + targetVertexId +
        ", partition=" + partition +
        '}';
  }
}
