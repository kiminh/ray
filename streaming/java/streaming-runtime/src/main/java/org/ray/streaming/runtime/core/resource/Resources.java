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
   * Allocate info
   * Key - container address
   * Value - Slot info:
   *         Key - slot id
   *         Value - vertex name
   */
  public Map<String, Map<Integer, List<String>>> allocatingMap = new HashMap<>(16);

  public int slotNumPerContainer = 0;
  public int currentContainerIndex = 0;
  public int currentContainerAllocatedNum = 0;
  public int capacityPerContainer = 0;

  public Resources(ResourceConfig resourceConfig) {
  }

  /**
   * Hot backup container list
   */
  public List<UniqueId> hotBackupNodes = new ArrayList<>();

  /**
   * Recently idle containers' id list
   */
  public List<UniqueId> recentlyIdleContainerIds = new ArrayList<>();

  public List<Container> getContainers() {
    return containerMap.values().stream().collect(Collectors.toList());
  }

  public Container getContainerByContainerId(ContainerID containerID) {
    return containerMap.values().stream().filter(container -> {
      return container.getId().equals(containerID);
    }).findFirst().get();
  }


  public Map<UniqueId, Map<String, Double>> getAllAvailableResource() {
    Map<UniqueId, Map<String, Double>> availableResourceMap = new HashMap<>();
    containerMap.values().stream()
        .forEach(container -> availableResourceMap
            .put(container.getNodeId(), container.getAvailableResource()));

    return availableResourceMap;
  }

  public int getCurrentSlotNum() {
    return containerMap.values().stream()
        .mapToInt(container -> container.getSlots().size()).sum();
  }

  public Map<UniqueId, List<Slot>> getContainerSlotsMap() {
    Map<UniqueId, List<Slot>> containerSlotsMap = new HashMap<>();
    containerMap.values().stream()
        .forEach(container -> containerSlotsMap.put(container.getNodeId(), container.getSlots()));
    return containerSlotsMap;
  }

  public Map<String, Integer> getAllocatedActorCounter() {
    Map<String, Integer> allocatedActorCounter = new HashMap<>();
    allocatingMap.entrySet().forEach(entry -> {
      int allocatedActorNum = entry.getValue().values().stream().mapToInt(List::size).sum();
      allocatedActorCounter.put(entry.getKey(), allocatedActorNum);
    });
    return allocatedActorCounter;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("containerMap", containerMap)
        .add("unhandledDeletedContainers", unhandledDeletedContainers)
        .add("allocatingMap", allocatingMap)
        .add("slotNumPerContainer", slotNumPerContainer)
        .add("capacityPerContainer", capacityPerContainer)
        .add("currentContainerIndex", currentContainerIndex)
        .add("currentContainerAllocatedNum", currentContainerAllocatedNum)
        .add("hotBackupNodes", hotBackupNodes)
        .add("recentlyIdleContainers", recentlyIdleContainerIds)
        .toString();
  }
}
