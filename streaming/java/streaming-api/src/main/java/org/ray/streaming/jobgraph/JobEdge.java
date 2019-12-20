package org.ray.streaming.jobgraph;

import java.io.Serializable;

import com.google.common.base.MoreObjects;

import org.ray.streaming.api.partition.Partition;

/**
 * JobEdge is connection and partition rules of upstream and downstream execution nodes.
 */
public class JobEdge implements Serializable {

  private int srcVertexId;
  private int targetVertexId;
  private Partition partition;

  public JobEdge(int srcVertexId, int targetVertexId, Partition partition) {
    this.srcVertexId = srcVertexId;
    this.targetVertexId = targetVertexId;
    this.partition = partition;
  }

  public int getSrcVertexId() {
    return srcVertexId;
  }

  public void setSrcVertexId(int srcVertexId) {
    this.srcVertexId = srcVertexId;
  }

  public int getTargetVertexId() {
    return targetVertexId;
  }

  public void setTargetVertexId(int targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  public Partition getPartition() {
    return partition;
  }

  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertexId", srcVertexId)
        .add("targetVertexId", targetVertexId)
        .add("partition", partition)
        .toString();
  }
}
