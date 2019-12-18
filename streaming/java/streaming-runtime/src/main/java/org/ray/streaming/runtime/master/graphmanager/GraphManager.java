package org.ray.streaming.runtime.master.graphmanager;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.ray.api.RayActor;

import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.OperatorNode;
import org.ray.streaming.runtime.core.graph.jobgraph.JobGraph;

public interface GraphManager extends Serializable {

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
