package org.ray.streaming.runtime.master.manager.graph;

import com.alipay.streaming.runtime.executiongraph.ExecutionEdge;
import com.alipay.streaming.runtime.executiongraph.ExecutionGraph;
import com.alipay.streaming.runtime.executiongraph.ExecutionJobVertex;
import com.alipay.streaming.runtime.executiongraph.ExecutionVertex;
import com.alipay.streaming.runtime.executiongraph.ExecutionVertexState;
import com.alipay.streaming.runtime.executiongraph.IntermediateResultPartition;
import com.alipay.streaming.runtime.executiongraph.OperatorNode;
import com.alipay.streaming.runtime.jobgraph.JobGraph;
import com.alipay.streaming.runtime.jobgraph.JobVertex;
import com.alipay.streaming.runtime.master.JobMaster;
import com.alipay.streaming.runtime.master.JobMasterRuntimeContext;
import com.alipay.streaming.runtime.monitor.event.payload.RescaleInfo;
import com.alipay.streaming.runtime.queue.impl.plasma.QueueUtils;
import com.alipay.streaming.runtime.resourcemanager.ContainerID;
import com.alipay.streaming.runtime.utils.GraphBuilder;
import com.alipay.streaming.runtime.utils.LoggerFactory;
import com.google.common.collect.Maps;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.ray.api.RayActor;
import org.ray.api.RayPyActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;

public class GraphManagerImpl implements GraphManager {

  private static final Logger LOG = LoggerFactory.getLogger(GraphManagerImpl.class);

  protected final JobMasterRuntimeContext runtimeContext;

  public GraphManagerImpl() {
    this.runtimeContext = null;
  }

  public GraphManagerImpl(JobMaster jobMaster) {
    this.runtimeContext = jobMaster.getRuntimeContext();
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
  public ExecutionGraph getChangedExecutionGraph() {
    if (getGraphs().getChangedExecutionGraph() == null) {
      resetChangedGraph();
    }
    return getGraphs().getChangedExecutionGraph();
  }

  @Override
  public ExecutionJobVertex findExecutionJobVertexByOpName(String opName) {
    return findExecutionJobVertexByOpNameInternal(getExecutionGraph(), opName);
  }

  @Override
  public ExecutionJobVertex findExecutionJobVertexByOpName4Update(String opName) {
    return findExecutionJobVertexByOpNameInternal(getChangedExecutionGraph(), opName);
  }

  private ExecutionJobVertex findExecutionJobVertexByOpNameInternal(ExecutionGraph executionGraph,
      String opName) {
    return executionGraph.getExeJobVertices().values().stream()
        .filter(executionJobVertex -> opName
            .equals(genOpNameWithIndex(executionJobVertex.getJobVertex()).getOpName()))
        .findFirst()
        .orElse(null);
  }

  @Override
  public Map<String, OperatorNode> getOperatorNodesFromCurrentGraph() {
    return getOperatorNodesInternal(false);
  }

  @Override
  public Map<String, OperatorNode> getOperatorNodesFromChangedGraph() {
    return getOperatorNodesInternal(true);
  }

  private Map<String, OperatorNode> getOperatorNodesInternal(boolean useChanged) {
    Map<String, OperatorNode> opIndexOpNodeMap = new HashMap<>();

    ExecutionGraph executionGraph;
    if (useChanged) {
      executionGraph = getChangedExecutionGraph();
    } else {
      executionGraph = getExecutionGraph();
    }

    for (ExecutionJobVertex exeJobVertex : executionGraph.getVerticesInCreationOrder()) {
      OperatorNode operatorNode = genOpNameWithIndex(exeJobVertex.getJobVertex());
      operatorNode.setParallelism(exeJobVertex.getParallelism());
      opIndexOpNodeMap.put(operatorNode.getOpIndex(), operatorNode);
    }
    return opIndexOpNodeMap;
  }

  @Override
  public Set<RayActor> findAllActorsByOpName(String opName) {
    LOG.info("Start to find all actors with specified operator name {}.", opName);
    Set<RayActor> resultSet = new HashSet<>();

    getExecutionGraph().getExeJobVertices().values().stream()
        .filter(executionJobVertex -> opName
            .equals(genOpNameWithIndex(executionJobVertex.getJobVertex()).getOpName()))
        .forEach(executionJobVertex -> executionJobVertex.getExeVertices().stream()
            .forEach(executionVertex -> resultSet.add(executionVertex.getActor())));

    LOG.info("Find all actors with specified operator name {} successfully. Result is {}.",
        opName, resultSet);
    return resultSet;
  }

  @Override
  public Set<RayActor> findSingleActor4Container() {
    LOG.info("Start to find single actor for every single container.");
    Set<RayActor> resultSet = new HashSet<>();
    Set<ContainerID> containerIdSet = new HashSet<>();

    boolean slotIsNotEmpty = false;
    for (ExecutionJobVertex executionJobVertex : getExecutionGraph().getExeJobVertices().values()) {
      for (ExecutionVertex executionVertex : executionJobVertex.getExeVertices()) {
        if (!slotIsNotEmpty) {
          // for npe case: slot is null when init
          if (null == executionVertex.getSlot()) {
            LOG.error("Can not find any actor for container when slot is null. "
                + "Please make sure this method is only used when job is running.");
            return null;
          } else {
            slotIsNotEmpty = true;
          }
        }

        ContainerID containerID = executionVertex.getSlot().getContainer().getId();
        if (!containerIdSet.contains(containerID)) {
          if (!(executionVertex.getActor() instanceof RayPyActor)) {
            // add container and actor only when 1st time matching
            containerIdSet.add(containerID);
            resultSet.add(executionVertex.getActor());
          }
        } else {
          continue;
        }
      }
    }
    LOG.info("Find single actor for every single container successfully. Result is {}.", resultSet);
    return resultSet;
  }

  @Override
  public int getOperatorParallelism(String opName) {
    ExecutionJobVertex exeJobVertex = findExecutionJobVertexByOpName(opName);
    return exeJobVertex.getParallelism();
  }

  @Override
  public ExecutionJobVertex updateOperatorParallelism(RescaleInfo rescaleInfo) {
    // find changed executionJobVertex by op name
    String opName = rescaleInfo.getOpName();
    int parallelismDelta = rescaleInfo.getParallelismDelta();

    ExecutionJobVertex changedExeJobVertex = findExecutionJobVertexByOpName4Update(opName);
    setChangedExecutionJobVertex(changedExeJobVertex);

    int oldParallelism = changedExeJobVertex.getParallelism();
    int newParallelism = oldParallelism + parallelismDelta;
    LOG.info("Update operator {} parallelism from {} to {}.", opName, oldParallelism,
        newParallelism);

    // update logical graph parallelism
    changedExeJobVertex.getJobVertex().setParallelism(newParallelism);

    // update execution graph parallelism
    if (parallelismDelta > 0) {
      changedExeJobVertex.scaleup(newParallelism, getChangedExecutionGraph());
    } else {
      changedExeJobVertex.scaledown(newParallelism, rescaleInfo.getSpecificIndexes());
    }

    // update execution graph max parallelism
    updateExecutionGraphMaxParallelism(getChangedExecutionGraph());

    return changedExeJobVertex;
  }

  private int updateExecutionGraphMaxParallelism(ExecutionGraph executionGraph) {
    int currentMaxParallelism = executionGraph.getExeJobVertices().values()
        .stream()
        .map(executionJobVertex  -> executionJobVertex.getParallelism())
        .max(Integer::compareTo).get();

    executionGraph.setMaxParallelism(currentMaxParallelism);

    return currentMaxParallelism;
  }

  public OperatorNode genOpNameWithIndex(JobVertex jobVertex) {
    String[] tokens = jobVertex.getName().split("-");
    String opIndex = tokens[0];
    String opName = tokens[1];
    OperatorNode operatorNode = new OperatorNode();
    operatorNode.setOpIndex(opIndex);
    operatorNode.setOpName(String.format("%s-%s", opIndex, opName.split("\n")[0]).trim());
    return operatorNode;
  }

  @Override
  public void resetChangedGraph() {
    setChangedExecutionGraph(getExecutionGraph().clone());
  }

  @Override
  public synchronized void refreshGraph() {
    LOG.info("Start to refresh graph.");
    refreshGraphInternal(getExecutionGraph());
  }

  @Override
  public synchronized void refreshChangedGraph() {
    LOG.info("Start to refresh changed graph.");
    refreshGraphInternal(getChangedExecutionGraph());
  }

  private void refreshGraphInternal(ExecutionGraph executionGraph) {
    List<ExecutionJobVertex> executionJobVertices = executionGraph
        .getVerticesInCreationOrder();
    for (ExecutionJobVertex executionJobVertex : executionJobVertices) {
      executionJobVertex.markAsNormal();

      List<ExecutionVertex> executionVertices = executionJobVertex.getExeVertices();
      int i = 0;
      Iterator itr = executionVertices.iterator();
      while (itr.hasNext()) {
        ExecutionVertex v = (ExecutionVertex) itr.next();
        if (v.getState() == ExecutionVertexState.TO_UPDATE
            || v.getState() == ExecutionVertexState.TO_ADD) {
          LOG.info("Mark execution vertex #{} as RUNNING, job vertex: {}.", i,
              executionJobVertex.getJobVertex().getName());
          v.setState(ExecutionVertexState.RUNNING);
        } else if (v.getState() == ExecutionVertexState.TO_DEL) {
          LOG.info("Remove execution vertex #{}, job vertex: {}.", i,
              executionJobVertex.getJobVertex().getName());
          itr.remove();
        }
        i++;
      }
    }
    LOG.info("Refresh graph finished.");
  }

  @Override
  public void updateGraphTemporarily(ExecutionGraph executionGraph) {
    getGraphs().setChangedExecutionGraph(executionGraph);
  }

  @Override
  public void updateGraph() {
    // 1. set new
    getGraphs().setExecutionGraph(getChangedExecutionGraph());

    // 2. reset changed to null
    getGraphs().setChangedExecutionGraph(null);
  }

  @Override
  public ExecutionJobVertex getChangedExecutionJobVertex() {
    return getGraphs().getChangedExecutionJobVertex();
  }

  @Override
  public Boolean isScalingUp() {
    return getGraphs().isScalingUp();
  }

  @Override
  public void setRescalingFlag(Boolean flag) {
    getGraphs().setScalingUp(flag);
  }

  private void setChangedExecutionJobVertex(ExecutionJobVertex executionJobVertex) {
    getGraphs().setChangedExecutionJobVertex(executionJobVertex);
  }

  private void setChangedExecutionGraph(ExecutionGraph executionGraph) {
    getGraphs().setChangedExecutionGraph(executionGraph);
  }

  @Override
  public void setupExecutionVertex(ExecutionGraph executionGraph) {
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
        if (edgeIsAlive(inputVertex, vertex)) {
          String queueName = genQueueName(inputVertex, vertex);
          inputQueues.put(inputVertex.getActorId(), queueName);
          inputActors.put(queueName, inputVertex.getActor());
        }
      }

      vertex.setInputQueues(inputQueues);
      vertex.setInputActors(inputActors);

      // create node output queues
      List<ExecutionVertex> outputVertices = vertex.getOutputExecutionVertices();
      for (ExecutionVertex outputVertex : outputVertices) {
        if (edgeIsAlive(vertex, outputVertex)) {
          String queueName = genQueueName(vertex, outputVertex);
          outputQueues.put(outputVertex.getActorId(), queueName);
          outputActors.put(queueName, outputVertex.getActor());
        }
      }
      vertex.setOutputQueues(outputQueues);
      vertex.setOutputActors(outputActors);

    });

    // set up relation
    setupQueueActorsRelation(executionGraph);

    LOG.info("Set up execution vertex end.");
  }

  private boolean edgeIsAlive(ExecutionVertex from , ExecutionVertex to) {
    return !from.isToDelete() && !to.isToDelete();
  }

  private String genQueueName(ExecutionVertex from, ExecutionVertex to) {
    return QueueUtils.genQueueName(
        from.getActorName(),
        to.getActorName(),
        from.getExeJobVertex().getBuildTime()
    );
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
      List<IntermediateResultPartition> partitions = curVertex.getResultPartitions();
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

    genTopoLevelOrder(executionGraph);
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
  private void genTopoLevelOrder(ExecutionGraph executionGraph) {
    executionGraph.getTopoLevelOrder().clear();

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
      executionGraph.getTopoLevelOrder().put(u.actor, u.level);
      for (ExecutionVertex v : outputMap.get(u.actor)) {
        inDegCnt.put(v.getActor(), inDegCnt.get(v.getActor()) - 1);
        if (inDegCnt.get(v.getActor()) == 0) {
          q.add(new SortNode(v.getActor(), u.level + 1));
        }
      }
    }

    LOG.info("Generate topology level order: {}.", executionGraph.getTopoLevelOrder());
  }
}
