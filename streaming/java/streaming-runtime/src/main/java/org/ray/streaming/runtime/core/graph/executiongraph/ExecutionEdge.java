package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import org.ray.streaming.api.partition.Partition;

/**
 *
 */
public class ExecutionEdge {

  private int srcJobVertexId;
  private int targetJobVertexId;
  private Partition partition;

  public ExecutionEdge(int srcJobVertexId, int targetJobVertexId, Partition partition) {
    this.srcJobVertexId = srcJobVertexId;
    this.targetJobVertexId = targetJobVertexId;
    this.partition = partition;
  }

  public int getSrcJobVertexId() {
    return srcJobVertexId;
  }

  public int getTargetJobVertexId() {
    return targetJobVertexId;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcJobVertexId", srcJobVertexId)
        .add("targetJobVertexId", targetJobVertexId)
        .add("partition", partition)
        .toString();
  }
}
