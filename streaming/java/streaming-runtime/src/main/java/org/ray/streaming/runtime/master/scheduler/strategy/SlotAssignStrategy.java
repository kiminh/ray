package org.ray.streaming.runtime.master.scheduler.strategy;

import java.util.List;
import java.util.Map;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.resource.Container;
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
  Map<String, Map<Integer, List<String>>> assignSlot(ExecutionGraph executionGraph);

  /**
   * Rebalance allocating map
   *
   * @param executionJobVertex execution job vertex
   * @param containerSlotsMap container -> slots map
   * @param containerResource container resources
   * @return HashMap, key: container address, value: HashMap (key: slotId, value: opName)
   */
  Map<String, Map<Integer, List<String>>> rebalance(ExecutionJobVertex executionJobVertex,
      Map<Container, List<Slot>> containerSlotsMap,
      Map<Container, Map<String, Double>> containerResource);

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