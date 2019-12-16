package org.ray.streaming.runtime.core.graph;

import java.io.Serializable;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.JobGraph;

public class Graphs implements Serializable {

  private JobGraph jobGraph;
  private ExecutionGraph executionGraph;
  private ExecutionGraph changedExecutionGraph;
  private ExecutionJobVertex changedExecutionJobVertex;
  private Boolean scaleingUp;

  public Graphs(JobGraph jobGraph,
      ExecutionGraph executionGraph) {
    this.jobGraph = jobGraph;
    this.executionGraph = executionGraph;
  }

  public JobGraph getJobGraph() {
    return jobGraph;
  }

  public void setJobGraph(JobGraph jobGraph) {
    this.jobGraph = jobGraph;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public void setExecutionGraph(ExecutionGraph executionGraph) {
    this.executionGraph = executionGraph;
  }

  public ExecutionGraph getChangedExecutionGraph() {
    return changedExecutionGraph;
  }

  public void setChangedExecutionGraph(
      ExecutionGraph changedExecutionGraph) {
    this.changedExecutionGraph = changedExecutionGraph;
  }

  public void setChangedExecutionJobVertex(ExecutionJobVertex changedExecutionJobVertex) {
    this.changedExecutionJobVertex = changedExecutionJobVertex;
  }

  public ExecutionJobVertex getChangedExecutionJobVertex() {
    return changedExecutionJobVertex;
  }

  public Boolean isScalingUp() {
    return scaleingUp;
  }

  public void setScalingUp(Boolean flag) {
    this.scaleingUp = flag;
  }
}
