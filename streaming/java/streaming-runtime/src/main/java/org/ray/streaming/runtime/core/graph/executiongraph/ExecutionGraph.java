package org.ray.streaming.runtime.core.graph.executiongraph;

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.core.graph.jobgraph.JobVertexID;
import org.ray.streaming.runtime.util.Serializer;

/**
 * ExecutionGraph is the physical plan for scheduling
 */
public class ExecutionGraph implements Serializable, Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

  private JobInformation jobInformation;
  private int maxParallelism;
  private Map<JobVertexID, ExecutionJobVertex> executionJobVertexMap;
  private List<ExecutionJobVertex> verticesInCreationOrder;
  private int lastExecutionVertexIndex = 0;
  private final long buildTime = System.currentTimeMillis();

  // Those fields will be initialized after job worker context were built
  private Map<String, Set<RayActor>> queueActorsMap = Maps.newHashMap();
  private Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap = Maps.newHashMap();
  private Map<RayActor, Integer> topologyLevelOrder = Maps.newHashMap();
  //viz graph
  private String digraph;

  public void setJobInformation(JobInformation jobInformation) {
    this.jobInformation = jobInformation;
  }

  public void setMaxParallelism(int maxParallelism) {
    LOG.info("Update max parallelism to: {}.", maxParallelism);

    this.maxParallelism = maxParallelism;

    Preconditions.checkArgument(executionJobVertexMap != null && !executionJobVertexMap.isEmpty(),
        "Execution job vertices is empty when setting max parallelism.");
    executionJobVertexMap.values().stream().forEach(executionJobVertex -> {
      executionJobVertex.setMaxParallelism(maxParallelism);
    });
  }

  public void setExecutionJobVertexMap(Map<JobVertexID, ExecutionJobVertex> executionJobVertexMap) {
    this.executionJobVertexMap = executionJobVertexMap;
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

  public Map<JobVertexID, ExecutionJobVertex> getExecutionJobVertexMap() {
    return executionJobVertexMap;
  }

  public void setQueueActorsMap(
      Map<String, Set<RayActor>> queueActorsMap) {
    this.queueActorsMap = queueActorsMap;
  }

  public Map<String, Set<RayActor>> getQueueActorsMap() {
    return queueActorsMap;
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

  public Map<RayActor, Integer> getTopologyLevelOrder() {
    return topologyLevelOrder;
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


  // ----------------------------------------------------------------------
  // Actor relation methods
  // ----------------------------------------------------------------------

  public List<ExecutionJobVertex> getSourceJobVertices() {
    return executionJobVertexMap.values().stream()
        .filter(ExecutionJobVertex::isSourceVertex)
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getSinkJobVertices() {
    return executionJobVertexMap.values().stream()
        .filter(ExecutionJobVertex::isSinkVertex)
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getNonSourceJobVertices() {
    return executionJobVertexMap.values().stream()
        .filter(jobVertex -> !jobVertex.isSourceVertex())
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getTransformJobVertices() {
    return executionJobVertexMap.values().stream()
        .filter(jobVertex -> !jobVertex.isSourceVertex() && !jobVertex.isSinkVertex())
        .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getAllExecutionJobVertices() {
    return executionJobVertexMap.values().stream().collect(Collectors.toList());
  }

  public List<ExecutionVertex> getAllExecutionVertices() {
    return getAllExecutionJobVertices().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<RayActor> getActorsFromJobVertices(List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
        .map(ExecutionJobVertex::getExecutionVertices)
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

  public List<RayActor> getAllActors() {
    return getActorsFromJobVertices(getAllExecutionJobVertices());
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("digraph", digraph)
        .toString();
  }
}
