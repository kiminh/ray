package org.ray.streaming.runtime.master.resourcemanager;

import java.util.List;
import java.util.Map;

import org.ray.api.RayActor;

import org.ray.streaming.runtime.config.Configuration;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.LanguageType;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;

public interface ResourceManager {

  List<Container> getRegisteredContainers();

  Map<String, Double> allocateActor(final ExecutionVertex exeVertex);

  void deallocateActor(final ExecutionVertex exeVertex);

  SlotAssignStrategy getSlotAssignStrategy();

  void setResources(Resources resources);

  Resources getResources();

  Map<Container, Map<String, Double>> getContainerResources();

  StreamingMasterConfig getConf();
}
