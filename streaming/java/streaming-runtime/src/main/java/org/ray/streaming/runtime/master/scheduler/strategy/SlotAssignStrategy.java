package org.ray.streaming.runtime.master.scheduler.strategy;

import java.util.List;
import java.util.Map;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;

/**
 *
 */
public interface SlotAssignStrategy {

  /**
   * Calucate slot number per container and set to resources.
   * @param containers
   * @param maxParallelism
   */
  int getSlotNumPerContainer(List<Container> containers, int maxParallelism);

  /**
   * Allocate slot to container
   * @param containers
   * @param slotNumPerContainer
   */
  void allocateSlot(final List<Container> containers, final int slotNumPerContainer);

  /**
   * Assign slot to execution vertex
   *
   * @param executionGraph execution graph
   * @return HashMap, key: container address, value: HashMap (key: slotId, value: opName)
   */
  Map<ContainerID, List<Slot>> assignSlot(ExecutionGraph executionGraph);

  /**
   * Get slot assign strategy name
   */
  String getName();

  /**
   * Update resources.
   * @param resources the specified resources
   */
  void updateResources(Resources resources);
}