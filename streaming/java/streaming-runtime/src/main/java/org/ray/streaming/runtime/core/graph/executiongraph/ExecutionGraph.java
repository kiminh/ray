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

  private List<ExecutionJobVertex> executionJobVertex;
  private List<RayActor<JobWorker>> sourceWorkers = new ArrayList<>();
  private List<RayActor<JobWorker>> sinkWorkers = new ArrayList<>();
  private long buildTime;

  public ExecutionGraph(List<ExecutionJobVertex> executionJobVertex) {
    this.executionJobVertex = executionJobVertex;
    for (ExecutionJobVertex jobVertex : executionJobVertex) {
      if (jobVertex.getJobVertexType() == JobVertexType.SOURCE) {
        List<RayActor<JobWorker>> actors = jobVertex.getExecutionVertex().stream()
            .map(ExecutionVertex::getWorker).collect(Collectors.toList());
        sourceWorkers.addAll(actors);
      }
      if (jobVertex.getJobVertexType() == JobVertexType.SINK) {
        List<RayActor<JobWorker>> actors = jobVertex.getExecutionVertex().stream()
            .map(ExecutionVertex::getWorker).collect(Collectors.toList());
        sinkWorkers.addAll(actors);
      }
    }
    this.buildTime = System.currentTimeMillis();
  }

  public List<RayActor<JobWorker>> getSourceWorkers() {
    return sourceWorkers;
  }

  public List<RayActor<JobWorker>> getSinkWorkers() {
    return sinkWorkers;
  }

  public List<ExecutionJobVertex> getExecutionJobVertex() {
    return executionJobVertex;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public ExecutionJobVertex getExecutionJobVertexByVertexId(int vertexId) {
    for (ExecutionJobVertex executionJobVertex : executionJobVertex) {
      for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertex()) {
        if (executionVertex.getVertexId() == vertexId) {
          return executionJobVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exits!");
  }

  public ExecutionVertex getExecutionVertexByVertexId(int vertexId) {
    for (ExecutionJobVertex jobVertex : executionJobVertex) {
      for (ExecutionVertex executionVertex : jobVertex.getExecutionVertex()) {
        if (executionVertex.getVertexId() == vertexId) {
          return executionVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exist!");
  }

  public Map<Integer, RayActor<JobWorker>> getTaskId2WorkerByJobVertexId(int jobVertexId) {
    for (ExecutionJobVertex jobVertex : executionJobVertex) {
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
