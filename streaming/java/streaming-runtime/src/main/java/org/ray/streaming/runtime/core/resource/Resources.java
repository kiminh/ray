package org.ray.streaming.runtime.core.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.ray.api.id.UniqueId;

import org.ray.streaming.runtime.config.master.ResourceConfig;

public class Resources {

  /**
   * NodeId -> Container
   */
  public Map<UniqueId, Container> containerMap = new ConcurrentHashMap<>(16);

  /**
   * Unhandled container del event
   */
  public List<Container> unhandledDeletedContainers = new LinkedList<>();

  /**
   * SlotAssignStrategy states
   */
  public Map<Container, Map<String, Double>> containerAvailableResource = new HashMap<>(16);
  public Map<String, Map<Integer, List<String>>> allocatingMap = new HashMap<>(16);
  public Map<Container, List<Slot>> containerSlotsMap = new HashMap<>(16);
  public int slotNumPerContainer = 0;
  public int currentContainerIndex = 0;
  public int currentContainerAllocatedNum = 0;

  public int capacityPerContainer = 0;
  public int rescalingCapacityPerContainer = 0;

  public Resources(ResourceConfig resourceConfig) {
    this.rescalingCapacityPerContainer = resourceConfig.customContainerCapacity();
  }

  /**
   * Hot backup container list
   */
  public List<UniqueId> hotBackupNodes = new ArrayList<>();

  /**
   * Recently idle containers' id list
   */
  public List<UniqueId> recentlyIdleContainerIds = new ArrayList<>();

  /**
   * Container allocation state
   */
  public ContainerAllocationState containerAllocationState = new ContainerAllocationState();

  public Resources() {
  }

  public List<Container> getContainers() {
    return containerMap.values().stream().collect(Collectors.toList());
  }

  public Map<String, Integer> getAllocatedActorCounter() {
    Map<String, Integer> allocatedActorCounter = new HashMap<>();
    allocatingMap.entrySet().forEach(entry -> {
      int allocatedActorNum = entry.getValue().values().stream().mapToInt(List::size).sum();
      allocatedActorCounter.put(entry.getKey(), allocatedActorNum);
    });
    return allocatedActorCounter;
  }

  public void clearRecentlyIdleContainers() {
    for (UniqueId nodeId : recentlyIdleContainerIds) {
      if (containerMap.containsKey(nodeId)) {
        Container container = containerMap.get(nodeId);
        if (null == container) {
          continue;
        }
        if (containerAvailableResource.containsKey(container)) {
          containerAvailableResource.remove(container);
        }
        if (containerSlotsMap.containsKey(container)) {
          containerSlotsMap.remove(container);
        }
        if (allocatingMap.containsKey(container.getAddress())) {
          allocatingMap.remove(container.getAddress());
        }
        containerMap.remove(nodeId);
      }
    }
    this.recentlyIdleContainerIds.clear();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("containerMap", containerMap)
        .add("unhandledDeletedContainers", unhandledDeletedContainers)
        .add("containerAvailableResource", containerAvailableResource)
        .add("allocatingMap", allocatingMap)
        .add("containerSlotsMap", containerSlotsMap)
        .add("slotNumPerContainer", slotNumPerContainer)
        .add("capacityPerContainer", capacityPerContainer)
        .add("rescalingCapacityPerContainer", rescalingCapacityPerContainer)
        .add("currentContainerIndex", currentContainerIndex)
        .add("currentContainerAllocatedNum", currentContainerAllocatedNum)
        .add("hotBackupNodes", hotBackupNodes)
        .add("recentlyIdleContainers", recentlyIdleContainerIds)
        .add("containerAllocationState", containerAllocationState)
        .toString();
  }
}
