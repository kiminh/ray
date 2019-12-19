package org.ray.streaming.runtime.core.graph.executiongraph;

import org.ray.api.RayActor;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 *
 */
public class ExecutionVertex {

  private int vertexId;
  private int vertexIndex;
  private RayActor<JobWorker> worker;

  public int getVertexId() {
    return vertexId;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }

  public RayActor<JobWorker> getWorker() {
    return worker;
  }

  @Override
  public String toString() {
    return "ExecutionVertex{" +
        "vertexId=" + vertexId +
        ", vertexIndex=" + vertexIndex +
        ", worker=" + worker +
        '}';
  }
}
