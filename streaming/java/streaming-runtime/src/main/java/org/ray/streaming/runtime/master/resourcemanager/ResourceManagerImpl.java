package org.ray.streaming.runtime.master.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.runtimecontext.NodeInfo;
import org.slf4j.Logger;

import com.alipay.streaming.runtime.remotecall.RemoteCallWorker;
import com.alipay.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.config.Configuration;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.LanguageType;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resource;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.JobMasterRuntimeContext;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategyFactory;
import org.ray.streaming.runtime.util.KryoUtils;
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

  private final ScheduledExecutorService scheduledExecutorService;
  private final JobMasterRuntimeContext runtimeContext;
  private final JobMaster jobMaster;
  private final Resources resources;

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

    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
    if (!TestHelper.isUTPattern()) {
      this.scheduledExecutorService.scheduleAtFixedRate(Ray.wrapRunnable(() -> updateResources()),
          CHECK_INTERVAL_SEC, CHECK_INTERVAL_SEC, TimeUnit.SECONDS);
    }
    LOG.info("ResourceManagerImpl init success.");
  }

  @Override
  public RayActor allocateActor(
      final Container container,
      final LanguageType language,
      final Configuration configuration,
      final ExecutionVertex exeVertex) {
    LOG.info("Start to allocate actor in container: {}.", container);

    // create actor
    Map<String, Double> userCustomResources = exeVertex.getExeJobVertex().getJobVertex()
        .getResources();
    LOG.info("User custom resource for vertex {} is: {}.", exeVertex.getTaskNameWithSubtask(),
        userCustomResources);
    Map<String, Double> resources = new HashMap<>(userCustomResources);
    String resourceKey = container.getName();
    resources.put(resourceKey, 1.0);

    // Using direct call actor when QUEUE_TYPE: StreamingQueue is specified.
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTIONS)
        .createActorCreationOptions();
    RayActor actor;
    if (LanguageType.JAVA == language) {
      actor = Ray.createActor(JobWorker::new,
          KryoUtils.writeToByteArray(exeVertex.getExecutionConfig().getConfiguration().toStringMap()),
          options);
    } else {
      Configuration executionVertexConfig = exeVertex.getExecutionConfig().getConfiguration();
      Map<String, Object> kvMap = executionVertexConfig.toMap();
      String jsonConfig = JSON.toJSONString(kvMap);
      actor = Ray.createPyActor(
          "streaming.runtime.core.worker.dynamic_py_worker",
          "DynamicPyWorker",
          jsonConfig.getBytes(),
          options);

      LOG.info("Call python actor init.");
      RayObject object = Ray.callPy((RayPyActor) actor, "init", jsonConfig.getBytes(),
          jsonConfig.getBytes());
      Ray.wait(Arrays.asList(object));
      LOG.info("Python actor init success.");
    }

    LOG.info("Allocate actor {} succeeded in container {}.", actor.getId(), container);
    return actor;
  }

  @Override
  public boolean deallocateActor(final RayActor actor) {
    LOG.info("Start deallocate actor {}.", actor.getId());

    // TODO: decrease container allocated actor num

    // ray call actor shutdown without reconstruction
    RemoteCallWorker.shutdownWithoutReconstruction(actor);
    LOG.info("Deallocate actor {} success.", actor.getId());
    return true;
  }

  @Override
  public List<Container> getRegisteredContainers() {
    return new ArrayList<>(resources.containerMap.values());
  }

  private void registerContainer(final NodeInfo nodeInfo) {
    LOG.info("Register container {}.", nodeInfo);

    Container container = new Container(nodeInfo.nodeAddress, nodeInfo.nodeId, nodeInfo.nodeHostname);

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

    // update container
    List<Container> occupiedContainers = new ArrayList<>();
    occupiedContainers.add(container);
    Resource resource = new Resource(occupiedContainers);
    container.setResource(resource);
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
    }
  }

  public void updateResources() {
    // get add&del nodes
    Map<UniqueId, NodeInfo> latestNodeInfos = RayUtils.getNodeInfoMap();
    List<UniqueId> addNodes = latestNodeInfos.keySet().stream()
        .filter(addr -> !resources.containerMap.containsKey(addr)).collect(Collectors.toList());
    List<UniqueId> delNodes = resources.containerMap.keySet().stream()
        .filter(nodeId -> !latestNodeInfos.containsKey(nodeId)).collect(Collectors.toList());

    if (!delNodes.isEmpty() && resources.hotBackupNodes.size() > 0) {
      // if delNodes container parts of hotBackupNodes, remove it
      delNodes.removeIf(delNode -> resources.hotBackupNodes.remove(delNode));

      // if container fo, parts of hotBackupNodes add to addNodes
      if (delNodes.size() > addNodes.size()) {
        int toIndex = Math.min(delNodes.size() - addNodes.size(), resources.hotBackupNodes.size());
        List<UniqueId> transferNodes = resources.hotBackupNodes.subList(0, toIndex);
        addNodes.addAll(transferNodes);
        LOG.info("TransferNodes {} add to addNodes {}.", transferNodes, addNodes);
        resources.hotBackupNodes.removeAll(transferNodes);
        saveResources();
      }
    }

    // update container info
    if (!addNodes.isEmpty() || !delNodes.isEmpty()) {
      LOG.info("Latest node infos: {}, containers: {}.", latestNodeInfos, resources.containerMap);
      LOG.info("Get add nodes info: {}, del nodes info: {}.", addNodes, delNodes);

      // get unregister containers
      for (UniqueId nodeId : delNodes) {
        Container deletedContainer = resources.containerMap.remove(nodeId);
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
      saveResources();
    }
  }

  /**
   * just for ut
   */
  public void clear() {
    resources.containerMap.clear();
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
  public void saveResources() {
    LOG.info("Save resources in runtime context: {}.", resources);
    if (TestHelper.isUTPattern()) {
      return;
    }
    Preconditions.checkArgument(jobMaster != null, "Job master is null when saving resources.");
    jobMaster.saveContext();
  }

  @Override
  public StreamingMasterConfig getConf() {
    return conf;
  }
}
