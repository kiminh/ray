package org.ray.streaming.runtime.master.scheduler.strategy.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertexState;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.util.LoggerFactory;

public class PipelineFirstStrategy implements SlotAssignStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(PipelineFirstStrategy.class);

  protected Resources resources;

  @Override
  public int getSlotNumPerContainer(List<Container> containers, int maxParallelism) {
    LOG.info("max parallelism: {}, container size: {}.", maxParallelism, containers.size());
    int slotNumPerContainer =
        (int) Math.ceil(Math.max(maxParallelism, containers.size()) * 1.0 / containers.size());
    LOG.info("slot num per container: {}.", slotNumPerContainer);
    return slotNumPerContainer;
  }

  /**
   * Allocate slot to target container, assume that we have 2 containers and max parallelism is 5,
   * the structure will be like:
   * <pre>
   * container_0
   *           |- slot_0
   *           |- slot_2
   *           |- slot_4
   * container_1
   *           |- slot_1
   *           |- slot_3
   *           |- slot_5
   * </pre>
   */
  @Override
  public void allocateSlot(List<Container> containers,
      int slotNumPerContainer) {
    int maxSlotSize = containers.size() * slotNumPerContainer;
    LOG.info("Allocate slot, maxSlotSize: {}.", maxSlotSize);

    for (int slotId = 0; slotId < maxSlotSize; ++slotId) {
      Container targetContainer = containers.get(slotId % containers.size());
      Slot slot = new Slot(slotId, targetContainer.getId());
      targetContainer.getSlots().add(slot);
    }

    // update new added containers' allocating map
    containers.forEach(c -> {
      List<Slot> slots = c.getSlots();
      Map<Integer, List<String>> slotActorMap = new HashMap<>();
      for (Slot s : slots) {
        slotActorMap.put(s.getId(), new ArrayList<>());
      }
      resources.allocatingMap.put(c.getAddress(), slotActorMap);
    });

    LOG.info("Allocate slot result: {}.", resources.allocatingMap);
  }

  @Override
  public Map<String, Map<Integer, List<String>>> assignSlot(ExecutionGraph executionGraph) {
    LOG.info("Container available resources: {}.", resources.getAllAvailableResource());
    Map<Integer, ExecutionJobVertex> vertices = executionGraph.getExecutionJobVertexMap();
    Map<Integer, Integer> vertexRemainingNum = new HashMap<>();
    vertices.forEach((k, v) -> {
      int size = v.getExecutionVertexList().size();
      vertexRemainingNum.put(k, size);
    });
    int totalExecutionVerticesNum = vertexRemainingNum.values().stream()
        .mapToInt(Integer::intValue)
        .sum();
    int containerNum = resources.containerMap.size();
    resources.capacityPerContainer = (int) Math
        .ceil(totalExecutionVerticesNum * 1.0 / containerNum);
    LOG.info("Total execution vertices num: {}, container num: {}, capacity per container: {}.",
        totalExecutionVerticesNum, containerNum, resources.capacityPerContainer);

    int maxParallelism = executionGraph.getMaxParallelism();

    for (int i = 0; i < maxParallelism; i++) {
      for (ExecutionJobVertex executionJobVertex : vertices.values()) {
        List<ExecutionVertex> exeVertices = executionJobVertex.getExecutionVertexList();
        // current job vertex assign finished
        if (exeVertices.size() <= i) {
          continue;
        }

        ExecutionVertex executionVertex = exeVertices.get(i);
        Container targetContainer = resources.getContainers().get(resources.currentContainerIndex);
        List<Slot> targetSlots = targetContainer.getSlots();
        allocate(executionVertex, targetContainer, targetSlots.get(i % targetSlots.size()));
      }
    }

    return resources.allocatingMap;
  }

  private void checkResource(Map<String, Double> requiredResource) {
    int checkedNum = 0;
    // if current container does not have enough resource, go to the next one (loop)
    while (!hasEnoughResource(requiredResource)) {
      checkedNum++;
      resources.currentContainerIndex =
          (resources.currentContainerIndex + 1) % resources.getContainers().size();
      Preconditions.checkArgument(checkedNum < resources.getContainers().size(),
          "No enough resource left, required resource: {}, available resource: {}.",
          requiredResource, resources.getAllAvailableResource());
      resources.currentContainerAllocatedNum = 0;
    }
  }

  private boolean hasEnoughResource(Map<String, Double> requiredResource) {
    LOG.info("Check resource for container, index: {}.", resources.currentContainerIndex);

    if (null == requiredResource) {
      return true;
    }

    Container currentContainer = resources.getContainers().get(resources.currentContainerIndex);
    Map<Integer, List<String>> slotActors = resources.allocatingMap.get(currentContainer.getAddress());
    if (slotActors != null && slotActors.size() > 0) {
      long allocatedActorNum = slotActors.values().stream().mapToLong(List::size).sum();
      if (allocatedActorNum  >= resources.capacityPerContainer) {
        LOG.info("Container remaining capacity is 0. used: {}, total: {}.", allocatedActorNum,
            resources.capacityPerContainer);
        return false;
      }
    }

    Map<String, Double> availableResource = currentContainer.getAvailableResource();
    for (Map.Entry<String, Double> entry : requiredResource.entrySet()) {
      if (availableResource.containsKey(entry.getKey())) {
        if (availableResource.get(entry.getKey()) < entry.getValue()) {
          LOG.warn("No enough resource for container {}. required: {}, available: {}.",
              currentContainer.getAddress(), requiredResource, availableResource);
          return false;
        }
      } else {
        LOG.warn("No enough resource for container {}. required: {}, available: {}.",
            currentContainer.getAddress(), requiredResource, availableResource);
        return false;
      }
    }
    return true;
  }

  private void decreaseResource(Map<String, Double> allocatedResource) {
    Container currentContainer = resources.getContainers().get(resources.currentContainerIndex);
    Map<String, Double> availableResource = currentContainer.getAvailableResource();

    allocatedResource.forEach((k, v) -> {
      Preconditions.checkArgument(availableResource.get(k) >= v,
          String.format("Available resource %s not >= decreased resource %s",
              availableResource.get(k), v));
      Double newValue = availableResource.get(k) - v;
      LOG.info("Decrease container {} resource [{}], from {} to {}.",
          currentContainer.getAddress(), k, availableResource.get(k), newValue);
      availableResource.put(k, newValue);
    });
  }

  private void allocate(ExecutionVertex vertex, Container container, Slot slot) {
    // set slot for execution vertex
    LOG.info("Set slot {} to vertex {}.", slot, vertex);
    vertex.setSlotIfNotExist(slot);

    // decrease available resource
    decreaseResource(vertex.getResources());

    // update allocating map
    resources.allocatingMap.get(container.getAddress()).get(slot.getId())
        .add(vertex.getVertexName());

    // current container reaches capacity limitation, go to the next one.
    resources.currentContainerAllocatedNum++;
    if (resources.currentContainerAllocatedNum >= resources.capacityPerContainer) {
      resources.currentContainerIndex =
          (resources.currentContainerIndex + 1) % resources.getContainers().size();
      resources.currentContainerAllocatedNum = 0;
    }
  }

  @Override
  public Map<String, Map<Integer, List<String>>> rebalance(ExecutionJobVertex executionJobVertex,
      Map<Container, List<Slot>> containerSlotsMap,
      Map<Container, Map<String, Double>> containerResource) {
    LOG.info("Start to rebalance. currentContainerIndex={}, currentContainerAllocatedNum={}.",
        resources.currentContainerIndex, resources.currentContainerAllocatedNum);

    Map<String, Double> requiredResource = executionJobVertex.getResources();
    for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertexList()) {
      if (executionVertex.getState() == ExecutionVertexState.TO_ADD) {
        checkResource(requiredResource);
        Container targetContainer = resources.getContainers().get(resources.currentContainerIndex);
        List<Slot> targetSlots = containerSlotsMap.get(targetContainer);
        allocate(executionVertex, targetContainer,
            targetSlots.get(executionVertex.getVertexIndex() % targetSlots.size()));
      } else if (executionVertex.getState() == ExecutionVertexState.TO_DEL) {
        reclaimResource(executionVertex);
      } else {
        continue;
      }
    }
    LOG.info("Rebalance finished.");
    return resources.allocatingMap;
  }

  private void reclaimResource(ExecutionVertex executionVertex) {
    Container container = resources.getContainerByContainerId(
        executionVertex.getSlot().getContainerID());
    Map<Integer, List<String>> slotActors = resources.allocatingMap.get(container.getAddress());
    String opName = executionVertex.getVertexName();
    for (Map.Entry<Integer, List<String>> entry : slotActors.entrySet()) {
      if (entry.getValue().contains(opName)) {
        entry.getValue().remove(opName);
        if (resources.getAllocatedActorCounter().get(container.getAddress()) == 0) {
          LOG.info("Container came to be idle, id: {}.", container.getNodeId());
          resources.recentlyIdleContainerIds.add(container.getNodeId());
        }
        break;
      }
    }
  }

  @Override
  public String getName() {
    return SlotAssignStrategyType.PIPELINE_FIRST_STRATEGY.getValue();
  }

  @Override
  public void updateResources(Resources resources) {
    this.resources = resources;
  }
}
