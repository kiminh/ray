package org.ray.streaming.runtime.core.graph.executiongraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ray.streaming.jobgraph.VertexType;
import org.ray.streaming.runtime.core.processor.StreamProcessor;

/**
 *
 */
public class ExecutionJobVertex {

  private int jobVertexId;
  private int parallelism;
  private ExecutionJobVertexType executionJobVertexType;
  private Map<String, Object> jobConfig;
  private List<ExecutionVertex> executionVertexList;

  private List<ExecutionJobEdge> inputEdges = new ArrayList<>();
  private List<ExecutionJobEdge> outputEdges = new ArrayList<>();

  private StreamProcessor streamProcessor;

  public enum ExecutionJobVertexType {
    SOURCE,
    PROCESS,
    SINK
  }

  public ExecutionJobVertex(int jobVertexId, int parallelism, Map<String, Object> jobConfig) {
    this.jobVertexId = jobVertexId;
    this.parallelism = parallelism;
    this.jobConfig = jobConfig;
    this.executionVertexList = createExecutionVertics();
  }

  private List<ExecutionVertex> createExecutionVertics() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    for (int index = 1; index <= parallelism; index++) {
      executionVertices.add(new ExecutionVertex(jobVertexId, index));
    }
    return executionVertices;
  }

  public int getJobVertexId() {
    return jobVertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public List<ExecutionVertex> getExecutionVertexList() {
    return executionVertexList;
  }

  public void setExecutionVertexList(
      List<ExecutionVertex> executionVertex) {
    this.executionVertexList = executionVertex;
  }

  public List<ExecutionJobEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionJobEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public List<ExecutionJobEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionJobEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public void setJobVertexType(
      VertexType vertexType) {
    switch (vertexType) {
      case SOURCE:
        this.executionJobVertexType = ExecutionJobVertexType.SOURCE;
        break;
      case PROCESS:
        this.executionJobVertexType = ExecutionJobVertexType.PROCESS;
        break;
      case SINK:
        this.executionJobVertexType = ExecutionJobVertexType.SINK;
        break;
        default:
          throw new IllegalStateException();
    }
  }

  public ExecutionJobVertexType getExecutionJobVertexType() {
    return executionJobVertexType;
  }


}
