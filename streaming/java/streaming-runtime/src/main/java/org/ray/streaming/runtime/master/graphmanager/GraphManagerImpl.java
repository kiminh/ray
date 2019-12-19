package org.ray.streaming.runtime.master.graphmanager;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Maps;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.jobgraph.JobEdge;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobVertex;
import org.slf4j.Logger;

import org.ray.streaming.runtime.core.graph.GraphBuilder;
import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.transfer.ChannelID;
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

    ExecutionGraph executionGraph = new ExecutionGraph();
    Collection<JobVertex> jobVertices = jobGraph.getJobVertexList();

    Map<Integer, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    for (JobVertex jobVertex : jobVertices) {
      int jobVertexId = jobVertex.getVertexId();
      exeJobVertexMap.put(jobVertexId,
          new ExecutionJobVertex(jobVertexId, jobVertex.getParallelism(), jobGraph.getJobConfig()));
    }

    // attach execution job vertex
    attachExecutionJobVertex(jobGraph.getJobEdgeList(), exeJobVertexMap);

    // set execution job vertex into execution graph
    executionGraph.setExecutionJobVertexMap(exeJobVertexMap);
    List<ExecutionJobVertex> executionJobVertexList = new ArrayList(
        executionGraph.getExecutionJobVertexMap().values());
    executionGraph.setExecutionJobVertexList(executionJobVertexList);

    // set max parallelism
    int maxParallelism = jobVertices.stream()
        .map(JobVertex::getParallelism)
        .max(Integer::compareTo).get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job config
    executionGraph.setJobConfig(jobGraph.getJobConfig());

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  private void attachExecutionJobVertex(List<JobEdge> jobEdgeList,
      Map<Integer, ExecutionJobVertex> exeJobVertexMap) {
    jobEdgeList.stream().forEach(jobEdge -> {
      ExecutionJobVertex producer = exeJobVertexMap.get(jobEdge.getSrcVertexId());
      ExecutionJobVertex consumer = exeJobVertexMap.get(jobEdge.getTargetVertexId());

      ExecutionJobEdge executionJobEdge =
          new ExecutionJobEdge(producer, consumer, jobEdge.getPartition());

      producer.getOutputEdges().add(executionJobEdge);
      consumer.getInputEdges().add(executionJobEdge);
    });
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

  @Override
  public void setupExecutionVertex() {
    ExecutionGraph executionGraph = getExecutionGraph();
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      LOG.info("Start to set up execution vertex[id:{}, worker:{}].",
          vertex.getId(), vertex.getActorName());

      LOG.info("Create input and output queues.");
      Map<ActorId, String> inputQueues = new HashMap<>();
      Map<String, RayActor> inputActors = new HashMap<>();
      Map<ActorId, String> outputQueues = new HashMap<>();
      Map<String, RayActor> outputActors = new HashMap<>();

      // create node input queues
      List<ExecutionVertex> inputVertices = vertex.getInputExecutionVertices();
      for (ExecutionVertex inputVertex : inputVertices) {
        String queueName = genQueueName(inputVertex, vertex);
        inputQueues.put(inputVertex.getActorId(), queueName);
        inputActors.put(queueName, inputVertex.getActor());
      }

      vertex.setInputQueues(inputQueues);
      vertex.setInputActors(inputActors);

      // create node output queues
      List<ExecutionVertex> outputVertices = vertex.getOutputExecutionVertices();
      for (ExecutionVertex outputVertex : outputVertices) {
        String queueName = genQueueName(vertex, outputVertex);
        outputQueues.put(outputVertex.getActorId(), queueName);
        outputActors.put(queueName, outputVertex.getActor());
      }
      vertex.setOutputQueues(outputQueues);
      vertex.setOutputActors(outputActors);

    });

    // set up relation
    setupQueueActorsRelation(executionGraph);

    LOG.info("Set up execution vertex end.");
  }

  private String genQueueName(ExecutionVertex from, ExecutionVertex to) {
    return ChannelID.genIdStr(
        from.getGlobalIndex(),
        to.getGlobalIndex(),
        from.getExecutionJobVertex().getBuildTime());
  }

  private void setupQueueActorsRelation(ExecutionGraph executionGraph) {
    LOG.info("Setup queue actors relation.");

    GraphBuilder graphBuilder = new GraphBuilder();
    Map<String, Set<RayActor>> queueActorsMap = Maps.newHashMap();
    Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap = Maps.newHashMap();

    executionGraph.getAllExecutionVertices().forEach(curVertex -> {

      // current
      actorIdExecutionVertexMap.put(curVertex.getActorId(), curVertex);

      // input
      List<ExecutionJobEdge> inputEdges = curVertex.getInputEdges();
      inputEdges.stream().filter(ExecutionJobEdge::isAlive).forEach(inputEdge -> {
        ExecutionVertex inputVertex = inputEdge.getSource().getProducer();
        String queueName = curVertex.getInputQueues().get(inputVertex.getActorId());
        setQueueActorRelation(queueActorsMap, queueName, inputVertex.getActor());

        graphBuilder.append(
            inputVertex.getActorId().toString(),
            curVertex.getActorId().toString(),
            queueName
        );

      });

      // output
      List<IntermediateResultPartition> partitions = curVertex.getOutputPartitions();
      partitions.stream().map(IntermediateResultPartition::getConsumers).flatMap(Collection::stream)
          .filter(ExecutionJobEdge::isAlive).forEach(outputEdge -> {
        ExecutionVertex outputVertex = outputEdge.getTarget();
        String queueName = curVertex.getOutputQueues().get(outputVertex.getActorId());
        setQueueActorRelation(queueActorsMap, queueName, outputVertex.getActor());
      });

      // isolated node
      if (curVertex.getInputQueues().isEmpty() &&
          curVertex.getOutputActors().isEmpty()) {
        graphBuilder.append(curVertex.getActorName());
      }
    });

    String digraph = graphBuilder.build();
    executionGraph.setDigraph(digraph);
    LOG.info("ExecutionGraph is : {}.", digraph);

    executionGraph.setQueueActorsMap(queueActorsMap);
    LOG.info("Queue Actors map is: {}.", queueActorsMap);

    executionGraph.setActorIdExecutionVertexMap(actorIdExecutionVertexMap);

    genTopologyLevelOrder(executionGraph);
  }

  private void setQueueActorRelation(
      Map<String, Set<RayActor>> queueActorMap,
      String queueName,
      RayActor actor) {

    Set<RayActor> actorSet = queueActorMap.computeIfAbsent(queueName, k -> new HashSet());
    actorSet.add(actor);
  }

  /**
   * generate the level order in toposort of each node. upstream nodes will always has smaller
   * order. nodes that have the same order share no edges.
   */
  private void genTopologyLevelOrder(ExecutionGraph executionGraph) {
    executionGraph.getTopologyLevelOrder().clear();

    class SortNode {

      private RayActor actor;
      private int level;

      private SortNode(RayActor actor, int level) {
        this.actor = actor;
        this.level = level;
      }
    }
    Map<RayActor, Integer> inDegCnt = new HashMap<>();
    Map<RayActor, List<ExecutionVertex>> outputMap = new HashMap<>();

    for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
      outputMap.put(vertex.getActor(), vertex.getOutputExecutionVertices());
      inDegCnt.put(vertex.getActor(), vertex.getInputExecutionVertices().size());
    }

    Queue<SortNode> q = new ArrayDeque<>();
    inDegCnt.forEach((actor, deg) -> {
      if (deg == 0) {
        q.add(new SortNode(actor, 0));
      }
    });
    while (!q.isEmpty()) {
      SortNode u = q.poll();
      executionGraph.getTopologyLevelOrder().put(u.actor, u.level);
      for (ExecutionVertex v : outputMap.get(u.actor)) {
        inDegCnt.put(v.getActor(), inDegCnt.get(v.getActor()) - 1);
        if (inDegCnt.get(v.getActor()) == 0) {
          q.add(new SortNode(v.getActor(), u.level + 1));
        }
      }
    }

    LOG.info("Generate topology level order: {}.", executionGraph.getTopologyLevelOrder());
  }
}
