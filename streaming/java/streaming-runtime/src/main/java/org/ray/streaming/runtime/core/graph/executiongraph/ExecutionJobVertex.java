package org.ray.streaming.runtime.core.graph.executiongraph;

import java.util.ArrayList;
import java.util.List;

import org.ray.streaming.plan.VertexType;
import org.ray.streaming.runtime.core.processor.StreamProcessor;

/**
 *
 */
public class ExecutionJobVertex {

  private int jobVertexId;
  private int parallelism;
  private JobVertexType jobVertexType;
  private List<ExecutionVertex> executionVertex = new ArrayList<>();

  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  private StreamProcessor streamProcessor;


  public ExecutionJobVertex(int jobVertexId, int parallelism) {
    this.jobVertexId = jobVertexId;
    this.parallelism = parallelism;
  }

  public int getJobVertexId() {
    return jobVertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public List<ExecutionVertex> getExecutionVertex() {
    return executionVertex;
  }

  public void setExecutionVertex(
      List<ExecutionVertex> executionVertex) {
    this.executionVertex = executionVertex;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public void setStreamProcessor(StreamProcessor streamProcessor) {
    this.streamProcessor = streamProcessor;
  }

  public StreamProcessor getStreamProcessor() {
    return streamProcessor;
  }

  public void setJobVertexType(
      VertexType vertexType) {
    switch (vertexType) {
      case SOURCE:
        this.jobVertexType = JobVertexType.SOURCE;
        break;
      case PROCESS:
        this.jobVertexType = JobVertexType.PROCESS;
        break;
      case SINK:
        this.jobVertexType = JobVertexType.SINK;
        break;
        default:
          throw new IllegalStateException();
    }
  }

  public JobVertexType getJobVertexType() {
    return jobVertexType;
  }

  @Override
  public String toString() {
    return "ExecutionJobVertex{" +
        "jobVertexId=" + jobVertexId +
        ", parallelism=" + parallelism +
        ", jobVertexType=" + jobVertexType +
        ", executionVertex=" + executionVertex +
        ", inputEdges=" + inputEdges +
        ", outputEdges=" + outputEdges +
        ", streamProcessor=" + streamProcessor +
        '}';
  }

  public enum JobVertexType {
    SOURCE,
    PROCESS,
    SINK
  }

}
