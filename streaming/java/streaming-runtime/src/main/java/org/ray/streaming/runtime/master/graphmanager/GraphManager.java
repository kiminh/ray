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
   * Get changed execution graph
   *
   * @return changed execution graph
   */
  ExecutionGraph getChangedExecutionGraph();

  /**
   * Get execution job vertex by specified op name with current graph
   *
   * @param opName: operator name
   * @return execution job vertex
   */
  ExecutionJobVertex findExecutionJobVertexByOpName(String opName);

  /**
   * Get execution job vertex by specified op name with changed graph
   *
   * @param opName: operator name
   * @return execution job vertex
   */
  ExecutionJobVertex findExecutionJobVertexByOpName4Update(String opName);

  /**
   * Get execution graph operator node
   *
   * @return operator index and OperatorNode map
   */
  Map<String, OperatorNode> getOperatorNodesFromCurrentGraph();

  /**
   * Get changed execution graph operator node
   *
   * @return operator index and OperatorNode map
   */
  Map<String, OperatorNode> getOperatorNodesFromChangedGraph();

  /**
   * Get ray actors by specified op name
   *
   * @param opName: operator name
   * @return ray actors set
   */
  Set<RayActor> findAllActorsByOpName(String opName);

  /**
   * Get ray actors for every container
   *
   * e.g. Container - 0: actor-1, actor-4, actor-5 and Container - 1: actor-0, actor-2, actor-3 will
   * return actor-1(4 or 5) and actor-0(2 or 3)
   *
   * @return ray actors set
   */
  Set<RayActor> findSingleActor4Container();

  /**
   * Get parallelism or specified operator
   *
   * @param opName: operator name
   * @return parallelism number
   */
  int getOperatorParallelism(String opName);

  /**
   * Update changed execution graph
   *
   * @param executionGraph
   */
  void updateGraphTemporarily(ExecutionGraph executionGraph);

  /**
   * Reset changed execution graph from current execution graph
   */
  void resetChangedGraph();

  /**
   * Update execution graph:
   * 1) back up old execution graph
   * 2) set new execution graph by temp changed execution graph
   * 3) reset temp changed execution graph
   */
  void updateGraph();

  /**
   * Refresh graph after scheduled or rescaled
   */
  void refreshGraph();

  /**
   * Refresh changed graph before scheduled or rescaled
   */
  void refreshChangedGraph();

  /**
   * get changed execution job vertex
   */
  ExecutionJobVertex getChangedExecutionJobVertex();

  /**
   * Create input and output info and setup relation for every vertex.
   * @param executionGraph target execution graph
   */
  void setupExecutionVertex(ExecutionGraph executionGraph);
}
