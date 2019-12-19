package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ray.api.RayActor;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex.JobVertexType;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 */
public class ExecutionGraph implements Serializable {

  private List<ExecutionJobVertex> executionJobVertexList;
  private Map<Integer, ExecutionJobVertex> executionJobVertexMap;
  private Map<String, Object> jobConfig;
  private int maxParallelism;
  private long buildTime;

  public ExecutionGraph() {
    this.buildTime = System.currentTimeMillis();
  }

  public List<ExecutionJobVertex> getExecutionJobVertexList() {
    return executionJobVertexList;
  }

  public void setExecutionJobVertexList(
      List<ExecutionJobVertex> executionJobVertexList) {
    this.executionJobVertexList = executionJobVertexList;
  }

  public Map<Integer, ExecutionJobVertex> getExecutionJobVertexMap() {
    return executionJobVertexMap;
  }

  public void setExecutionJobVertexMap(
      Map<Integer, ExecutionJobVertex> executionJobVertexMap) {
    this.executionJobVertexMap = executionJobVertexMap;
  }

  public Map<String, Object> getJobConfig() {
    return jobConfig;
  }

  public void setJobConfig(Map<String, Object> jobConfig) {
    this.jobConfig = jobConfig;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public void setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public ExecutionVertex getExecutionJobVertexByJobVertexId(int vertexId) {
    for (ExecutionJobVertex jobVertex : executionJobVertexList) {
      for (ExecutionVertex executionVertex : jobVertex.getExecutionVertex()) {
        if (executionVertex.getVertexId() == vertexId) {
          return executionVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exist!");
  }

  public Map<Integer, RayActor<JobWorker>> getTaskId2WorkerByJobVertexId(int jobVertexId) {
    for (ExecutionJobVertex jobVertex : executionJobVertexList) {
      if (jobVertex.getJobVertexId() == jobVertexId) {
        Map<Integer, RayActor<JobWorker>> vertexId2Worker = new HashMap<>();
        for (ExecutionVertex executionVertex : jobVertex.getExecutionVertex()) {
          vertexId2Worker.put(executionVertex.getVertexId(), executionVertex.getWorker());
        }
        return vertexId2Worker;
      }
    }
    throw new RuntimeException("ExecutionJobVertex " + jobVertexId + " does not exist!");
  }

}
