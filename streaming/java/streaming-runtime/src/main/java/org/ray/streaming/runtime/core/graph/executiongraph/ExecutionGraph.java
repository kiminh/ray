package org.ray.streaming.runtime.core.graph.executiongraph;

import com.alipay.streaming.runtime.jobgraph.JobVertexID;
import com.alipay.streaming.runtime.utils.LoggerFactory;
import com.alipay.streaming.runtime.utils.Serializer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;

/**
 * ExecutionGraph is the physical plan for scheduling
 */
public class ExecutionGraph implements Serializable, Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

  private JobInformation jobInformation;
  private int maxParallelism;
  private Map<JobVertexID, ExecutionJobVertex> exeJobVertices;
  private List<ExecutionJobVertex> verticesInCreationOrder;
  private int lastExecutionVertexIndex = 0;
  private final long buildTime = System.currentTimeMillis();

  // Those fields will be initialized after job worker context were built
  private Map<String, Set<RayActor>> queueActorsMap = Maps.newHashMap();
  private Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap = Maps.newHashMap();
  private Map<RayActor, Integer> topoLevelOrder = Maps.newHashMap();
  private String digraph;

  public void setJobInformation(JobInformation jobInformation) {
    this.jobInformation = jobInformation;
  }

  public void setMaxParallelism(int maxParallelism) {
    LOG.info("Update max parallelism to: {}.", maxParallelism);

    this.maxParallelism = maxParallelism;

    Preconditions.checkArgument(exeJobVertices != null && !exeJobVertices.isEmpty(),
        "Execution job vertices is empty when setting max parallelism.");
    exeJobVertices.values().stream().forEach(executionJobVertex -> {
      executionJobVertex.setMaxParallelism(maxParallelism);
    });
  }

  public void setExeJobVertices(Map<JobVertexID, ExecutionJobVertex> exeJobVertices) {
    this.exeJobVertices = exeJobVertices;
  }

  public void setVerticesInCreationOrder(List<ExecutionJobVertex> verticesInCreationOrder) {
    this.verticesInCreationOrder = verticesInCreationOrder;
  }

  public JobInformation getJobInformation() {
    return jobInformation;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public Map<JobVertexID, ExecutionJobVertex> getExeJobVertices() {
    return exeJobVertices;
  }

  public List<ExecutionJobVertex> getVerticesInCreationOrder() {
    return verticesInCreationOrder;
  }

  public int incLastExecutionVertexIndex() {
    return lastExecutionVertexIndex++;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public String getDigraph() {
    return digraph;
  }

  public void setDigraph(String digraph) {
    this.digraph = digraph;
  }

  public Map<RayActor, Integer> getTopoLevelOrder() {
    return topoLevelOrder;
  }

  public void setQueueActorsMap(
      Map<String, Set<RayActor>> queueActorsMap) {
    this.queueActorsMap = queueActorsMap;
  }

  public void setActorIdExecutionVertexMap(
      Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap) {
    this.actorIdExecutionVertexMap = actorIdExecutionVertexMap;
  }

  @Override
  public ExecutionGraph clone() {
    byte[] executionGraphBytes = Serializer.encode(this);
    return Serializer.decode(executionGraphBytes);
  }

  public List<ExecutionVertex> getAllNewbornVertices() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    getVerticesInCreationOrder().stream()
        .map(ExecutionJobVertex::getNewbornVertices)
        .forEach(executionVertices::addAll);
    return executionVertices;
  }

  public List<ExecutionVertex> getAllMoribundVertices() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    getVerticesInCreationOrder().stream()
        .map(ExecutionJobVertex::getMoribundVertices)
        .forEach(executionVertices::addAll);
    return executionVertices;
  }

  public Map<String, String> getJobConf() {
    return jobInformation.getJobConf();
  }

  // ----------------------------------------------------------------------
  // Actor relation methods
  // ----------------------------------------------------------------------

  public List<ExecutionJobVertex> getSourceJobVertices() {
    return exeJobVertices.values().stream()
        .filter(ExecutionJobVertex::isSourceVertex)
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getSinkJobVertices() {
    return exeJobVertices.values().stream()
        .filter(ExecutionJobVertex::isSinkVertex)
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getNonSourceJobVertices() {
    return exeJobVertices.values().stream()
        .filter(jobVertex -> !jobVertex.isSourceVertex())
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getTransformJobVertices() {
    return exeJobVertices.values().stream()
        .filter(jobVertex -> !jobVertex.isSourceVertex() && !jobVertex.isSinkVertex())
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getAllJobVertices() {
    return exeJobVertices.values().stream().collect(Collectors.toList());
  }

  public List<ExecutionVertex> getAllExecutionVertices() {
    return getAllJobVertices().stream()
        .map(ExecutionJobVertex::getExeVertices)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<RayActor> getActorsFromJobVertices(List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
        .map(ExecutionJobVertex::getExeVertices)
        .flatMap(Collection::stream)
        .map(ExecutionVertex::getActor)
        .collect(Collectors.toList());
  }

  public ExecutionVertex getExecutionVertexByActorId(ActorId actorId) {
    return actorIdExecutionVertexMap.get(actorId);
  }

  public List<RayActor> getSourceActors() {
    return getActorsFromJobVertices(getSourceJobVertices());
  }

  public List<RayActor> getSinkActors() {
    return getActorsFromJobVertices(getSinkJobVertices());
  }

  public List<RayActor> getNonSourceActors() {
    return getActorsFromJobVertices(getNonSourceJobVertices());
  }

  public List<RayActor> getTransformActors() {
    return getActorsFromJobVertices(getTransformJobVertices());
  }

  public List<RayActor> getAllActors() {
    return getActorsFromJobVertices(getAllJobVertices());
  }

  public Map<ActorId, RayActor> getSourceActorsMap() {
    final Map<ActorId, RayActor> actorsMap = new HashMap<>();
    getSourceActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public Map<ActorId, RayActor> getSinkActorsMap() {
    final Map<ActorId, RayActor> actorsMap = new HashMap<>();
    getSinkActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public Map<ActorId, RayActor> getTransformActorsMap() {
    final Map<ActorId, RayActor> actorsMap = new HashMap<>();
    getTransformActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public List<ActorId> getAllActorsId() {
    return getAllActors().stream()
        .map(actor -> actor.getId())
        .collect(Collectors.toList());
  }

  /**
   * get all down stream actors. for example:
   * <pre>
   * actorA - actorB - actorD - actorF
   *          \
   *           actorC - actorE
   * </pre>
   * if para is actorA, we will only return actorB & actorC & actorD & actorE & actorF.
   *
   * @param actor actor
   * @return direct down stream actors
   */
  public Set<RayActor> getAllDownStreamActors(RayActor actor) {

    Queue<RayActor> queue = new ArrayDeque<>();
    queue.add(actor);

    Set<RayActor> actorSet = new HashSet<>();
    while (!queue.isEmpty()) {
      RayActor qActor = queue.poll();

      ExecutionVertex vertex = actorIdExecutionVertexMap.get(qActor.getId());
      if (vertex != null) {
        Set<RayActor> outputActors = vertex.getOutputExecutionVertices().stream()
            .map(outputVertex -> outputVertex.getActor())
            .collect(Collectors.toSet());
        queue.addAll(outputActors);
        actorSet.addAll(outputActors);
      }
    }

    return Collections.unmodifiableSet(actorSet);
  }

  public Set<RayActor> getActorsByQueueName(String queueName) {
    return queueActorsMap.getOrDefault(queueName, Sets.newHashSet());
  }

  public Set<String> getQueuesByActor(RayActor actor) {
    return queueActorsMap.entrySet().stream()
        .filter(entry -> entry.getValue().contains(actor))
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet());
  }

  public String getQueueNameByActor(RayActor actor1, RayActor actor2) {
    // only for test, very slow, could be optimized
    final String[] res = new String[1];
    queueActorsMap.forEach((queue, actorSet) -> {
      if (actorSet.contains(actor1) && actorSet.contains(actor2)) {
        res[0] = queue;
      }
    });
    return res[0];
  }

  public RayActor getAnotherActor(RayActor qActor, String queueName) {
    Set<RayActor> set = getActorsByQueueName(queueName);
    final RayActor[] res = new RayActor[1];
    set.forEach(actor -> {
      if (!actor.equals(qActor)) {
        res[0] = actor;
      }
    });
    return res[0];
  }

  public int getTopoLevelOrder(RayActor actor) {
    return topoLevelOrder.get(actor);
  }

  public Optional<RayActor> getActorById(ActorId actorId) {
    return getAllActors().stream()
        .filter(actor -> actor.getId().equals(actorId))
        .findFirst();
  }

  public Set<String> getActorName(Set<ActorId> actorIds) {
    return getAllExecutionVertices().stream()
        .filter(executionVertex -> actorIds.contains(executionVertex.getActorId()))
        .map(executionVertex -> executionVertex.getActorName())
        .collect(Collectors.toSet());
  }

  public String getActorName(ActorId actorId) {
    Set<ActorId> set = Sets.newHashSet();
    set.add(actorId);
    Set<String> result = getActorName(set);
    if (result.isEmpty()) {
      return null;
    }
    return result.iterator().next();
  }

  public Map<String, String> getActorTags(ActorId actorId) {
    Map<String, String> actorTags = new HashMap<>();
    actorTags.put("actor_id", actorId.toString());
    Optional<RayActor> rayActor = getActorById(actorId);
    if (rayActor.isPresent()) {
      ExecutionVertex executionVertex = getExecutionVertexByActorId(actorId);
      actorTags.put("op_name", executionVertex.getOpInfo().opName);
      actorTags.put("op_index", String.valueOf(executionVertex.getOpInfo().opIndex));
    }
    return actorTags;
  }

  public Map<String, String> getActorTags(RayActor actor) {
    return getActorTags(actor.getId());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("digraph", digraph)
        .toString();
  }
}
