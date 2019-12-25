package org.ray.streaming.runtime.master.resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.ray.api.Ray;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resource;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.JobMasterRuntimeContext;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategyFactory;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.util.RayUtils;
import org.ray.streaming.runtime.util.TestHelper;

public class ResourceManagerImpl implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerImpl.class);

  private static final long CHECK_INTERVAL_SEC = 1;
  // TODO: get capacity from config
  private static final int CAPACITY = 500;
  private static final String CONTAINER_ENGAGED_KEY = "CONTAINER_ENGAGED_KEY";
  private static final String CONTAINER_HOT_BACKUP_KEY = "CONTAINER_HOT_BACKUP_KEY";

  private StreamingMasterConfig conf;
  private SlotAssignStrategy slotAssignStrategy;

  private final JobMasterRuntimeContext runtimeContext;
  private final JobMaster jobMaster;
  private final Resources resources;

  /**
   * Thread
   */
  private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(
      1, new BasicThreadFactory
      .Builder().namingPattern("updateResource-schedule-pool-%d").daemon(true).build());

  public ResourceManagerImpl(JobMaster jobMaster) {
    this.runtimeContext = jobMaster.getRuntimeContext();
    this.jobMaster = jobMaster;
    this.resources = runtimeContext.getResources();
    this.conf = runtimeContext.getConf().masterConfig;
    boolean autoScalingEnable = !TestHelper.isUTPattern();
    LOG.info("ResourceManagerImpl begin init, conf is {}, resources are {}, autoScalingEnable is {}.",
        conf.resourceConfig, resources, autoScalingEnable);

    SlotAssignStrategyType slotAssignStrategyType =
        SlotAssignStrategyType.valueOf(conf.schedulerConfig.slotAssignStrategy().toUpperCase());
    slotAssignStrategy = SlotAssignStrategyFactory.getStrategy(slotAssignStrategyType);
    slotAssignStrategy.setResources(resources);
    LOG.info("Slot assign strategy: {}.", slotAssignStrategy.getName());

    updateResources();

    if (!TestHelper.isUTPattern()) {
      scheduledExecutorService.scheduleAtFixedRate(Ray.wrapRunnable(() -> {
            Ray.wrapRunnable(() -> updateResources());
          }),
          CHECK_INTERVAL_SEC,
          CHECK_INTERVAL_SEC,
          TimeUnit.SECONDS
      );
    }
    LOG.info("ResourceManagerImpl init success.");
  }

  @Override
  public Map<String, Double> allocateResource(final ExecutionVertex executionVertex) {
    Container container = executionVertex.getSlot().getContainer();
    LOG.info("Start to allocate resource for actor with container: {}.", container);

    // allocate resource to actor
    Map<String, Double> resources = new HashMap<>();
    resources.put(container.getName(), 1.0);

    LOG.info("Allocate resource to actor [vertexId={}] succeeded with container {}.",
        executionVertex.getVertexId(), container);
    return resources;
  }

  @Override
  public void deallocateResource(final ExecutionVertex executionVertex) {
    LOG.info("Start deallocate resource for actor {}.", executionVertex.getWorkerActorId());

    // TODO: decrease container allocated actor num

    LOG.info("Deallocate resource for actor {} success.", executionVertex.getWorkerActorId());
  }

  @Override
  public List<Container> getRegisteredContainers() {
    return new ArrayList<>(resources.containerMap.values());
  }

  private void registerContainer(final NodeInfo nodeInfo) {
    LOG.info("Register container {}.", nodeInfo);

    Container container = new Container(nodeInfo.nodeAddress, nodeInfo.nodeId, nodeInfo.nodeHostname);
    container.setAvailableResource(nodeInfo.resources);

    // handle deleted containers
    if (!resources.unhandledDeletedContainers.isEmpty()) {
      Container deletedContainer = resources.unhandledDeletedContainers.remove(0);
      container.setId(deletedContainer.getId());
      LOG.info("Deleted container {} reset resource success, left {} deleted containers to handle.",
          deletedContainer, resources.unhandledDeletedContainers.size());
    }

    // create ray resource
    Ray.setResource(container.getNodeId(), container.getName(), CAPACITY);
    Ray.setResource(container.getNodeId(), CONTAINER_ENGAGED_KEY, 1);

    // update container map
    resources.containerMap.put(container.getNodeId(), container);
  }

  private void unregisterContainer(final Container container, boolean isLoss) {
    LOG.info("Unregister container {}, isLoss: {}.", container, isLoss);

    if (isLoss) {
      LOG.error("Container {} is lost.", container);
    } else {
      // delete resource with capacity=0
      Ray.setResource(container.getNodeId(), container.getName(), 0);
      Ray.setResource(container.getNodeId(), CONTAINER_ENGAGED_KEY, 0);

      // remove from container map
      resources.containerMap.remove(container.getNodeId());
    }
  }

  public void updateResources() {
    // get add&del nodes
    Map<UniqueId, NodeInfo> latestNodeInfos = RayUtils.getNodeInfoMap();
    List<UniqueId> addNodes = latestNodeInfos.keySet().stream()
        .filter(addr -> !resources.containerMap.containsKey(addr)).collect(Collectors.toList());
    List<UniqueId> delNodes = resources.containerMap.keySet().stream()
        .filter(nodeId -> !latestNodeInfos.containsKey(nodeId)).collect(Collectors.toList());

    // update container info
    if (!addNodes.isEmpty() || !delNodes.isEmpty()) {
      LOG.info("Latest node infos: {}, containers: {}.", latestNodeInfos, resources.containerMap);
      LOG.info("Get add nodes info: {}, del nodes info: {}.", addNodes, delNodes);

      // get unregister containers
      for (UniqueId nodeId : delNodes) {
        Container deletedContainer = resources.containerMap.get(nodeId);
        resources.unhandledDeletedContainers.add(deletedContainer);
        LOG.info("Remove container {} from container list.", deletedContainer);
      }

      // register containers
      for (UniqueId nodeId : addNodes) {
        registerContainer(latestNodeInfos.get(nodeId));
      }

      // unregister containers
      if (!delNodes.isEmpty()) {
        for (Container deletedContainer : resources.unhandledDeletedContainers) {
          unregisterContainer(deletedContainer, true);
        }
      }
    }
  }

  @Override
  public SlotAssignStrategy getSlotAssignStrategy() {
    return slotAssignStrategy;
  }

  @Override
  public void setResources(Resources resources) {
    this.runtimeContext.setResources(resources);
  }

  @Override
  public Resources getResources() {
    return this.resources;
  }

  @Override
  public Map<Container, Map<String, Double>> getContainerResources() {
    Map<Container, Map<String, Double>> containerResources = new HashMap<>();
    Map<UniqueId, NodeInfo> nodeInfoMap = RayUtils.getNodeInfoMap();
    LOG.info("Node info map is: {}.", nodeInfoMap);
    nodeInfoMap.forEach((k, v) ->
        containerResources.put(resources.containerMap.get(k), v.resources));
    return containerResources;
  }

  @Override
  public StreamingMasterConfig getConf() {
    return conf;
  }
}
