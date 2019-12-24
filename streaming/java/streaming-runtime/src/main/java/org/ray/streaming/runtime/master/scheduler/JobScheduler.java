package org.ray.streaming.runtime.master.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertexState;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;

public class JobScheduler implements IJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

  private static final String TASK_NODE_ID = "NodeId";
  private static final String TASK_NODE_ID_SPILIT = "-";
  private static final String TASK_NODE_ID_PREFIX = "node-";
  private static final String TASK_INDEX = "Index";
  private static final String TASK_ID = "TaskId";
  private static final String TASK_ACTOR_ID = "ActorId";

  private List<Container> containers;
  private StreamingConfig jobConf;

  private final JobMaster jobMaster;
  private final ResourceManager resourceManager;
  private final GraphManager graphManager;
  private final WorkerLifecycleController workerController;
  private final SlotAssignStrategy strategy;

  private Long lastPartialCheckpointId = 1L;
  private Set<ActorId> waitingCommitActorIds = new HashSet<>();

  public JobScheduler(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.graphManager = jobMaster.getGraphManager();
    this.resourceManager = jobMaster.getResourceManager();
    this.workerController = jobMaster.getWorkerController();
    this.strategy = resourceManager.getSlotAssignStrategy();
    this.jobConf = jobMaster.getRuntimeContext().getConf();

    // get containers
    containers = this.resourceManager.getRegisteredContainers();
    Preconditions.checkState(containers != null && !containers.isEmpty(),
        "containers is invalid: %s", containers);

    LOG.info("Scheduler init success.");
  }

  // ----------------------------------------------------------------------
  // Scheduling at startup
  // ----------------------------------------------------------------------

  @Override
  public boolean scheduleJob(ExecutionGraph executionGraph) {
    LOG.info("Start to schedule job: {}.", executionGraph.getJobName());

    // get max parallelism
    int maxParallelism = executionGraph.getMaxParallelism();

    // get containers
    containers = this.resourceManager.getRegisteredContainers();
    Preconditions.checkState(containers != null && !containers.isEmpty(),
        "containers is invalid: %s", containers);

    // allocate slot
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers, maxParallelism);
    resourceManager.getResources().slotNumPerContainer = slotNumPerContainer;
    LOG.info("Slot num per container: {}.", slotNumPerContainer);

    strategy.allocateSlot(containers, slotNumPerContainer);
    LOG.info("Container slot map is: {}.", resourceManager.getResources().getContainerSlotsMap());

    // assign slot
    Map<String, Map<Integer, List<String>>> allocatingMap = strategy.assignSlot(executionGraph);
    LOG.info("Allocating map is: {}.", JSON.toJSONString(allocatingMap));

    // start all new added workers
    createWorkers(executionGraph);

    // register worker context and start to run
    run(executionGraph);

    return true;
  }

  private void createWorkers(ExecutionGraph executionGraph) {
    // set worker config
    executionGraph.getAllAddedExecutionVertices().stream().forEach(executionVertex -> {
      Map<String, String> conf = setWorkerConfig(jobConf.workerConfigTemplate, executionVertex);
      LOG.info("Worker {} conf is {}.", executionVertex.getVertexIndex(), conf);
    });

    // Create JobWorker actors
    executionGraph.getAllAddedExecutionVertices().stream()
        .forEach(vertex -> {
          // allocate by resource manager
          Map<String, Double> resources = resourceManager.allocateResource(vertex);

          // create actor by controller
          workerController.createWorker(vertex, resources);

          // update state
          vertex.setState(ExecutionVertexState.RUNNING);
        });
  }

  /**
   * Start to execute job
   * @param executionGraph
   */
  private void run(ExecutionGraph executionGraph) {
    initWorkers(executionGraph, jobConf.workerConfigTemplate);
    initMaster();
    startAllWorkers();
  }

  private void startAllWorkers() {
    jobMaster.startAllWorkers();
  }

  private void initMaster() {
    jobMaster.init(false);
  }

  private void initWorkers(ExecutionGraph executionGraph,
      final StreamingWorkerConfig configTemplate) {
    LOG.info("Begin initiating workers.");

    RayActor<JobMaster> masterActor = jobMaster.getJobMasterActor();

    // setup vertex
    graphManager.setupExecutionVertex(executionGraph);

    // register worker context
    long waitStartTime = System.currentTimeMillis();
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      JobWorkerContext ctx = buildJobWorkerContext(vertex, masterActor);
      boolean initResult = workerController.initWorker(vertex.getWorkerActor(), ctx);

      if (initResult) {
        LOG.error("Init workers occur error.");
        return;
      }
    });

    long waitEndTime = System.currentTimeMillis();
    LOG.info("Finish initiating workers. Cost {} ms.", waitEndTime - waitStartTime);
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex,
      RayActor<JobMaster> masterActor) {

    // create worker context
    JobWorkerContext ctx = new JobWorkerContext(
        executionVertex.getWorkerActorId(),
        masterActor,
        KryoUtils.writeToByteArray(executionVertex)
    );

    return ctx;
  }

  private Map<String, String> setWorkerConfig(StreamingWorkerConfig workerConfigTemplate,
      ExecutionVertex executionVertex) {
    Map<String, String> workerConfMap = new HashMap<>();

    // pass worker config template (common part)
    workerConfMap.putAll(workerConfigTemplate.configMap);

    // TODO: extra config

    return workerConfMap;
  }
}
