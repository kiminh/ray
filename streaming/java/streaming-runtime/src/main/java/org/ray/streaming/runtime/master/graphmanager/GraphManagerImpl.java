package org.ray.streaming.runtime.master.graphmanager;

import com.google.common.collect.Maps;
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

import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.JobInformation;
import org.ray.streaming.runtime.core.graph.jobgraph.JobEdge;
import org.ray.streaming.runtime.core.graph.jobgraph.JobVertexID;
import org.ray.streaming.runtime.core.transfer.ChannelID;
import org.slf4j.Logger;

import org.ray.streaming.runtime.core.graph.GraphBuilder;
import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.IntermediateResultPartition;
import org.ray.streaming.runtime.core.graph.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.jobgraph.JobVertex;
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
   * @param jobGraph logical execution plan
   * @return physical execution plan
   */
  private ExecutionGraph buildExecutionGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    ExecutionGraph executionGraph = new ExecutionGraph();
    Collection<JobVertex> jobVertices = jobGraph.getVertices();

    Map<JobVertexID, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    for (JobVertex jobVertex : jobVertices) {
      exeJobVertexMap.put(jobVertex.getId(),
          new ExecutionJobVertex(jobVertex, executionGraph, jobGraph.getJobConfig()));
    }

    for (JobVertex jobVertex : jobVertices) {
      attachExecutionJobVertex(jobVertex, exeJobVertexMap);
    }

    for (ExecutionJobVertex exeJobVertex : exeJobVertexMap.values()) {
      exeJobVertex.attachExecutionVertex();
    }

    executionGraph.setExecutionJobVertexMap(exeJobVertexMap);

    List<ExecutionJobVertex> verticesInCreationOrder = new ArrayList<>(
        executionGraph.getExecutionJobVertexMap().values());
    executionGraph.setVerticesInCreationOrder(verticesInCreationOrder);
    int maxParallelism = jobVertices.stream().map(JobVertex::getParallelism)
        .max(Integer::compareTo).get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job information
    JobInformation jobInformation = new JobInformation(
        jobGraph.getJobName(),
        jobGraph.getJobConfig());
    executionGraph.setJobInformation(jobInformation);

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  private static void attachExecutionJobVertex(JobVertex jobVertex,
      Map<JobVertexID, ExecutionJobVertex> exeJobVertexMap) {
    ExecutionJobVertex exeJobVertex = exeJobVertexMap.get(jobVertex.getId());

    // get input edges
    List<JobEdge> inputs = jobVertex.getInputs();

    // attach execution job vertex
    for (JobEdge input : inputs) {
      JobVertex producer = input.getSource().getProducer();
      ExecutionJobVertex producerEjv = exeJobVertexMap.get(producer.getId());
      exeJobVertex.connectNewIntermediateResultAsInput(input, producerEjv);
    }
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
      List<ExecutionEdge> inputEdges = curVertex.getInputEdges();
      inputEdges.stream().filter(ExecutionEdge::isAlive).forEach(inputEdge -> {
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
          .filter(ExecutionEdge::isAlive).forEach(outputEdge -> {
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
