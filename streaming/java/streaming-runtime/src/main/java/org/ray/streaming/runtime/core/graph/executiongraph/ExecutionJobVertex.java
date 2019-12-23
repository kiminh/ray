package org.ray.streaming.runtime.core.graph.executiongraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobVertex;
import org.ray.streaming.jobgraph.LanguageType;
import org.ray.streaming.jobgraph.VertexType;

import org.ray.streaming.runtime.core.processor.StreamProcessor;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical job vertex which including parallelism execution vertex.
 */
public class ExecutionJobVertex {

  private final String jobName;
  private final int jobVertexId;
  private final String jobVertexName;
  private final JobVertex jobVertex;
  private int parallelism;
  private Map<String, String> jobConfig;
  private long buildTime;
  private List<ExecutionVertex> executionVertexList;

  private List<ExecutionJobEdge> inputEdges = new ArrayList<>();
  private List<ExecutionJobEdge> outputEdges = new ArrayList<>();

  private StreamProcessor streamProcessor;

  public ExecutionJobVertex(String jobName, JobVertex jobVertex, Map<String, String> jobConfig,
      long buildTime) {
    this.jobName = jobName;
    this.jobVertexId = jobVertex.getVertexId();
    this.jobVertexName = jobVertex.getVertexName();
    this.jobVertex = jobVertex;
    this.parallelism = jobVertex.getParallelism();
    this.jobConfig = jobConfig;
    this.buildTime = buildTime;
    this.executionVertexList = createExecutionVertics();
  }

  private List<ExecutionVertex> createExecutionVertics() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    for (int index = 1; index <= parallelism; index++) {
      executionVertices.add(new ExecutionVertex(jobVertexId, index, this));
    }
    return executionVertices;
  }

  public Map<Integer, RayActor<JobWorker>> getExecutionVertexWorkers () {
    Map<Integer, RayActor<JobWorker>> executionVertexWorkersMap = new HashMap<>();

    Preconditions.checkArgument(
        executionVertexList != null && !executionVertexList.isEmpty(),
        "Empty execution vertex.");
    executionVertexList.stream().forEach(vertex -> {
      Preconditions.checkArgument(
          vertex.getWorkerActor() != null,
          "Empty execution vertex worker actor.");
      executionVertexWorkersMap.put(vertex.getVertexId(), vertex.getWorkerActor());
    });

    return executionVertexWorkersMap;
  }

  public String getJobName() {
    return jobName;
  }

  public int getJobVertexId() {
    return jobVertexId;
  }

  public String getJobVertexName() {
    return jobVertexName;
  }

  public JobVertex getJobVertex() {
    return jobVertex;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
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

  public void setStreamProcessor(StreamProcessor streamProcessor) {
    this.streamProcessor = streamProcessor;
  }

  public StreamProcessor getStreamProcessor() {
    return streamProcessor;
  }

  public LanguageType getLanguageType() {
    return jobVertex.getLanguageType();
  }

  public VertexType getVertexType() {
    return jobVertex.getVertexType();
  }

  public long getBuildTime() {
    return buildTime;
  }

  public boolean isSourceVertex() {
    return getVertexType() == VertexType.SOURCE;
  }

  public boolean isProcessVertex() {
    return getVertexType() == VertexType.PROCESS;
  }

  public boolean isSinkVertex() {
    return getVertexType() == VertexType.SINK;
  }
}
