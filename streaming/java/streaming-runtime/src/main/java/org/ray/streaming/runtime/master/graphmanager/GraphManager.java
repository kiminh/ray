package org.ray.streaming.runtime.master.graphmanager;

import java.io.Serializable;

import org.ray.streaming.jobgraph.JobGraph;

import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

public interface GraphManager extends Serializable {

  /**
   * Build execution graph from job graph.
   * @param jobGraph
   * @return
   */
  ExecutionGraph buildExecutionGraph(JobGraph jobGraph);

  /**
   * Set up execution job vertex.
   * @param jobGraph logical plan
   * @return
   */
  ExecutionGraph setupExecutionJobVertex(JobGraph jobGraph);

  /**
   * Setup execution vertex.
   * @param executionGraph physical plan
   * @return
   */
  ExecutionGraph setupExecutionVertex(ExecutionGraph executionGraph);

  /**
   * Get graphs
   *
   * @return all graphs
   */
  Graphs getGraphs();

  /**
   * Get job graph
   *
   * @return job graph
   */
  JobGraph getJobGraph();

  /**
   * Get current execution graph
   *
   * @return current execution graph
   */
  ExecutionGraph getExecutionGraph();
}
