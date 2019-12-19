package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class ExecutionVertex implements Serializable {

  private final int vertexId;
  private final int vertexIndex;

  private List<ExecutionEdge> inputEdges;
  private List<ExecutionEdge> outputEdges;

  public ExecutionVertex(int jobVertexId, int index) {
    this.vertexId = generateExecutionVertexId(jobVertexId, index);
    this.vertexIndex = index;
  }

  private int generateExecutionVertexId(int jobVertexId, int index) {
    return jobVertexId * 100000 + index;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }
}
