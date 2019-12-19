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

  /**
   * Create input and output info and setup relation for every vertex.
   */
  void setupExecutionVertex();
}
