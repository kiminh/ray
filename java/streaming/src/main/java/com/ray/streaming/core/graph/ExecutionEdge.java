package com.ray.streaming.core.graph;

import com.ray.streaming.api.partition.IPartition;
import java.io.Serializable;

public class ExecutionEdge implements Serializable {

  private int srcNodeId;
  private int targetNodeId;
  private IPartition partition;

  public ExecutionEdge(int srcNodeId, int targetNodeId, IPartition partition) {
    this.srcNodeId = srcNodeId;
    this.targetNodeId = targetNodeId;
    this.partition = partition;
  }

  public int getSrcNodeId() {
    return srcNodeId;
  }

  public void setSrcNodeId(int srcNodeId) {
    this.srcNodeId = srcNodeId;
  }

  public int getTargetNodeId() {
    return targetNodeId;
  }

  public void setTargetNodeId(int targetNodeId) {
    this.targetNodeId = targetNodeId;
  }

  public IPartition getPartition() {
    return partition;
  }

  public void setPartition(IPartition partition) {
    this.partition = partition;
  }

  public String getStream() {
    return "stream:" + srcNodeId + "-" + targetNodeId;
  }
}
