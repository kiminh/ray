package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

public class ExecutionEdge implements Serializable {
  private final IntermediateResultPartition source;
  private final ExecutionVertex target;
  private final int inputIndex;

  public ExecutionEdge(IntermediateResultPartition source,
      ExecutionVertex target, int inputIndex) {
    this.source = source;
    this.target = target;
    this.inputIndex = inputIndex;
  }

  public IntermediateResultPartition getSource() {
    return source;
  }

  public ExecutionVertex getTarget() {
    return target;
  }

  public int getInputIndex() {
    return inputIndex;
  }

  public boolean isAlive() {
    return !getSource().getProducer().isToDelete() && !getTarget().isToDelete();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("target", target)
        .add("inputIndex", inputIndex)
        .toString();
  }
}