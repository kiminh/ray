package org.ray.streaming.runtime.master.graphmanager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobVertex;
import org.slf4j.Logger;

import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.JobMasterRuntimeContext;
import org.ray.streaming.runtime.util.LoggerFactory;

public class GraphManagerImpl implements GraphManager {

  private static final Logger LOG = LoggerFactory.getLogger(GraphManagerImpl.class);

  protected final JobMasterRuntimeContext runtimeContext;

  public GraphManagerImpl() {
    this.runtimeContext = null;
  }

  public GraphManagerImpl(JobMaster jobMaster, JobGraph jobGraph) {
    this.runtimeContext = jobMaster.getRuntimeContext();
    runtimeContext.setGraphs(jobGraph, buildExecutionGraph(jobGraph));
  }

  /**
   * Logical execution plan transforms physical execution plan.
   * @param jobGraph
   * @return
   */
  @Override
  public ExecutionGraph buildExecutionGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    // setup execution job vertex
    ExecutionGraph executionGraph = setupExecutionJobVertex(jobGraph);

    // set max parallelism
    int maxParallelism = jobGraph.getJobVertexList().stream()
        .map(JobVertex::getParallelism)
        .max(Integer::compareTo).get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job config
    executionGraph.setJobConfig(jobGraph.getJobConfig());

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  @Override
  public ExecutionGraph setupExecutionJobVertex(JobGraph jobGraph) {
    ExecutionGraph executionGraph = new ExecutionGraph();

    // create vertex
    Map<Integer, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    for (JobVertex jobVertex : jobGraph.getJobVertexList()) {
      int jobVertexId = jobVertex.getVertexId();
      exeJobVertexMap.put(jobVertexId,
          new ExecutionJobVertex(jobVertexId, jobVertex.getParallelism(), jobGraph.getJobConfig()));
    }

    // attach vertex
    jobGraph.getJobEdgeList().stream().forEach(jobEdge -> {
      ExecutionJobVertex producer = exeJobVertexMap.get(jobEdge.getSrcVertexId());
      ExecutionJobVertex consumer = exeJobVertexMap.get(jobEdge.getTargetVertexId());

      ExecutionJobEdge executionJobEdge =
          new ExecutionJobEdge(producer, consumer, jobEdge.getPartition());

      producer.getOutputEdges().add(executionJobEdge);
      consumer.getInputEdges().add(executionJobEdge);
    });

    // set execution job vertex into execution graph
    executionGraph.setExecutionJobVertexMap(exeJobVertexMap);
    List<ExecutionJobVertex> executionJobVertexList = new ArrayList(
        executionGraph.getExecutionJobVertexMap().values());
    executionGraph.setExecutionJobVertexList(executionJobVertexList);

    return executionGraph;
  }

  @Override
  public ExecutionGraph setupExecutionVertex(ExecutionGraph executionGraph) {

    LOG.info("Set up execution vertex end.");
    return executionGraph;
  }

  @Override
  public Graphs getGraphs() {
    return runtimeContext.getGraphs();
  }

  @Override
  public JobGraph getJobGraph() {
    return getGraphs().getJobGraph();
  }

  @Override
  public ExecutionGraph getExecutionGraph() {
    return getGraphs().getExecutionGraph();
  }

  private void setQueueActorRelation(
      Map<String, Set<RayActor>> queueActorMap,
      String queueName,
      RayActor actor) {

    Set<RayActor> actorSet = queueActorMap.computeIfAbsent(queueName, k -> new HashSet());
    actorSet.add(actor);
  }
}
