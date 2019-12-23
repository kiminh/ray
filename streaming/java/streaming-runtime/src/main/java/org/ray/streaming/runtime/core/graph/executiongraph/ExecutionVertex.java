package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.jobgraph.LanguageType;
import org.ray.streaming.jobgraph.VertexType;

import org.ray.streaming.runtime.core.processor.StreamProcessor;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical vertex for every parallelism.
 */
public class ExecutionVertex implements Serializable {

  private final int vertexId;
  private final int vertexIndex;
  private final String vertexName;
  private final ExecutionJobVertex executionJobVertex;

  private RayActor<JobWorker> workerActor;
  private Slot slot;
  private List<ExecutionEdge> inputEdges;
  private List<ExecutionEdge> outputEdges;

  public ExecutionVertex(int jobVertexId, int index, ExecutionJobVertex executionJobVertex) {
    this.vertexId = generateExecutionVertexId(jobVertexId, index);
    this.vertexIndex = index;
    this.vertexName = executionJobVertex.getJobVertexName() + "-" + vertexIndex;
    this.executionJobVertex = executionJobVertex;
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

  public ExecutionJobVertex getExecutionJobVertex() {
    return executionJobVertex;
  }

  public RayActor<JobWorker> getWorkerActor() {
    return workerActor;
  }

  public ActorId getWorkerActorId() {
    return workerActor.getId();
  }

  public void setWorkerActor(RayActor<JobWorker> workerActor) {
    this.workerActor = workerActor;
  }

  public Slot getSlot() {
    return slot;
  }

  public void setSlot(Slot slot) {
    this.slot = slot;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public VertexType getVertextType() {
    return executionJobVertex.getVertexType();
  }

  public Map<String, String> getJobConfig() {
    return executionJobVertex.getJobConfig();
  }

  public LanguageType getLanguageType() {
    return executionJobVertex.getLanguageType();
  }

  public long getBuildTime() {
    return executionJobVertex.getBuildTime();
  }

  public int getParallelism() {
    return executionJobVertex.getParallelism();
  }

  public StreamProcessor getStreamProcessor() {
    return executionJobVertex.getStreamProcessor();
  }

  public boolean isSourceVertex() {
    return executionJobVertex.isSourceVertex();
  }

  public boolean isProcessVertex() {
    return executionJobVertex.isProcessVertex();
  }

  public boolean isSinkVertex() {
    return executionJobVertex.isSinkVertex();
  }

  public String getVertexName() {
    return vertexName;
  }
}
