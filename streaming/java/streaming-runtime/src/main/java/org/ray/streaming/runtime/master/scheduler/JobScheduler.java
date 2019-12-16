package org.ray.streaming.runtime.master.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.config.types.OperatorType;
import org.ray.streaming.runtime.config.internal.WorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.worker.JobWorkerContext;

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
    this.workerController = new WorkerLifecycleController(resourceManager);
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
    LOG.info("Start to schedule job: {}.", executionGraph.getJobInformation().getJobName());

    // get max parallelism
    int maxParallelism = executionGraph.getMaxParallelism();

    // get containers
    containers = this.resourceManager.getRegisteredContainers();
    Preconditions.checkState(containers != null && !containers.isEmpty(),
        "containers is invalid: %s", containers);

    // allocate slot and update RM context
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers, maxParallelism);
    resourceManager.getResources().slotNumPerContainer = slotNumPerContainer;
    LOG.info("Slot num per container: {}.", slotNumPerContainer);

    Map<Container, List<Slot>> containerSlotsMap = strategy.allocateSlot(containers,
        slotNumPerContainer);
    resourceManager.getResources().containerSlotsMap = containerSlotsMap;
    LOG.info("Container slot map is: {}.", containerSlotsMap);

    // assign slot
    Map<String, Map<Integer, List<String>>> allocatingMap = strategy.assignSlot(executionGraph,
        resourceManager.getResources().containerSlotsMap, resourceManager.getContainerResources());
    LOG.info("Allocating map is: {}.", JSON.toJSONString(allocatingMap));

    // start all new added workers
    createWorkers(executionGraph);

    // register worker context and start to run
    run(executionGraph);

    return true;
  }

  private void createWorkers(ExecutionGraph executionGraph) {
    // set worker config
    executionGraph.getAllExecutionVertices().stream().forEach(executionVertex -> {
      Map<String, String> conf = setWorkerConfig(jobConf.workerConfigTemplate, executionVertex);
      LOG.info("Worker {} conf is {}.", executionVertex.getActorName(), conf);
    });

    // Create JobWorker actors
    workerController.createWorkers(executionGraph.getAllNewbornVertices());
  }

  /**
   * Start to execute job
   * @param executionGraph
   */
  private void run(ExecutionGraph executionGraph) {
    registerWorkersContext(executionGraph, jobConf.workerConfigTemplate);
    registerMasterContext();
    startAllWorkers();
  }

  private void registerMasterContext() {
    jobMaster.registerContext(false);
  }

  private void startAllWorkers() {
    jobMaster.startAllWorkers();
  }

  /**
   * Register workers context
   * @param executionGraph
   */
  private void registerWorkersContext(ExecutionGraph executionGraph,
      final StreamingWorkerConfig configTemplate) {
    LOG.info("Begin register worker context.");

    RayActor<JobMaster> masterActor = jobMaster.getJobMasterActor();
    List<RayObject<Object>> rayObjects = new ArrayList<>();

    // setup vertex
    graphManager.setupExecutionVertex(executionGraph);

    // register worker context
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      JobWorkerContext ctx = buildJobWorkerContext(vertex, configTemplate, masterActor);
      rayObjects.add(RemoteCallWorker.registerContext(vertex.getActor(), ctx));
    });

    long waitStartTime = System.currentTimeMillis();
    List<ObjectId> waitingRayObjectIds = rayObjects.stream().map(x -> x.getId())
        .collect(Collectors.toList());
    Ray.get(waitingRayObjectIds);
    long waitEndTime = System.currentTimeMillis();
    LOG.info("Finish register worker context. cost {} ms.", waitEndTime - waitStartTime);
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex,
      StreamingWorkerConfig configTemplate,
      RayActor<JobMaster> masterActor) {

    // create worker context
    JobWorkerContext ctx = new JobWorkerContext(
        configTemplate.commonConfig.jobName(),
        executionVertex.getOpNameWithIndex(),
        executionVertex.getActorName(),
        masterActor,
        executionVertex.getActorId(),
        executionVertex.getExecutionConfig().getConfiguration().toStringMap(),
        executionVertex.getInputQueues(),
        executionVertex.getOutputQueues(),
        executionVertex.getInputActors(),
        executionVertex.getOutputActors(),
        KryoUtils.writeToByteArray(executionVertex)
    );

    // update sub dag
    updateRoleInChangedSubDagIfNeeded(ctx, executionVertex);

    return ctx;
  }

  private void updateRoleInChangedSubDagIfNeeded(JobWorkerContext ctx,
      ExecutionVertex executionVertex) {
    ExecutionJobVertex executionJobVertex = executionVertex.getExeJobVertex();

    if (!executionJobVertex.isChangedOrAffected()) {
      LOG.info("ExecutionJobVertex is not changed or affected: {}.", executionJobVertex);
      return;
    }

    LOG.info("ExecutionJobVertex is changed or affected: {}.", executionJobVertex);
    switch (executionJobVertex.getExecutionJobVertexState()) {
      case AFFECTED_UP_STREAM:
      case AFFECTED_NEIGHBOUR_PARENT:
        executionVertex.setRoleInChangedSubDag(OperatorType.SOURCE);
        ctx.markAsChanged();
        break;
      case AFFECTED_DOWN_STREAM:
        executionVertex.setRoleInChangedSubDag(OperatorType.SINK);
        ctx.markAsChanged();
        break;
      case AFFECTED_NEIGHBOUR:
        executionVertex.setRoleInChangedSubDag(OperatorType.TRANSFORM);
        ctx.markAsChanged();
        break;
      case CHANGED:
        handleChangedNode(ctx, executionVertex);
        break;
      case NORMAL:
      default:
        break;
    }
    ctx.roleInChangedSubDag = executionVertex.getRoleInChangedSubDag();
  }

  private void handleChangedNode(JobWorkerContext ctx, ExecutionVertex executionVertex) {
    ctx.markAsChanged();

    ExecutionJobVertex executionJobVertex = executionVertex.getExeJobVertex();
    if (executionVertex.getExeJobVertex().isSourceVertex()) {
      executionVertex.setRoleInChangedSubDag(OperatorType.SOURCE);
    } else if (executionJobVertex.isSinkVertex()) {
      executionVertex.setRoleInChangedSubDag(OperatorType.SINK);
    } else if (executionJobVertex.isSourceAndSinkVertex()) {
      executionVertex.setRoleInChangedSubDag(OperatorType.SOURCE_AND_SINK);
    } else {
      executionVertex.setRoleInChangedSubDag(OperatorType.TRANSFORM);
    }

    ctx.roleInChangedSubDag = executionVertex.getRoleInChangedSubDag();
  }

  private Map<String, String> setWorkerConfig(StreamingWorkerConfig workerConfigTemplate,
      ExecutionVertex executionVertex) {
    Map<String, String> workerConfMap = new HashMap<>();

    // pass worker config template (common part)
    workerConfMap.putAll(workerConfigTemplate.configMap);

    // set operator type of queue
    if (executionVertex.isSourceVertex()) {
      workerConfMap.put(WorkerConfig.OPERATOR_TYPE_INTERNAL, OperatorType.SOURCE.name());
    } else if (executionVertex.isSinkVertex()) {
      workerConfMap.put(WorkerConfig.OPERATOR_TYPE_INTERNAL, OperatorType.SINK.name());
    } else {
      workerConfMap.put(WorkerConfig.OPERATOR_TYPE_INTERNAL, OperatorType.TRANSFORM.name());
    }

    // worker id
    workerConfMap.put(WorkerConfig.WORKER_ID_INTERNAL,
        executionVertex.getExecutionConfig().getWorkerId());

    // worker name
    workerConfMap.put(WorkerConfig.WORKER_NAME_INTERNAL, executionVertex.getActorName());

    // op name
    workerConfMap.put(WorkerConfig.OPERATOR_NAME_INTERNAL, executionVertex.getOpNameWithIndex());

    // job name
    workerConfMap.put(WorkerConfig.JOB_NAME_INTERNAL, workerConfigTemplate.commonConfig.jobName());

    // reliability level
    workerConfMap.put(WorkerConfig.RELIABILITY_LEVEL_INTERNAL,
        workerConfigTemplate.reliabilityConfig.reliabilityLevel());

    // compatible python
    compatiblePythonWorkerConfig(workerConfMap, workerConfigTemplate);

    // set conf map into vertex
    executionVertex.updateJobConfig(workerConfMap);

    return workerConfMap;
  }

  /**
   * The following key and value must be equal with StreamingConstants in python
   * package: python.streaming.runtime.core.constant
   * py: streaming_constants.py
   */
  private void compatiblePythonWorkerConfig(Map<String, String> workerConfMap,
      StreamingWorkerConfig workerConfigTemplate) {
    workerConfMap.put(WorkerConfig.PY_CP_MODE,
        "save_checkpoint_" + workerConfigTemplate.checkpointConfig.cpMode());
    workerConfMap.put(WorkerConfig.PY_CP_MODE_PY,
        "save_checkpoint_" + workerConfigTemplate.checkpointConfig.cpMode() + "_py");
    workerConfMap.put(WorkerConfig.PY_CP_STATE_BACKEND_TYPE,
        "cp_state_backend_" + workerConfigTemplate.stateBackendConfig.stateBackendType());
    workerConfMap.put(WorkerConfig.PY_CP_PANGU_CLUSTER_NAME,
        workerConfigTemplate.stateBackendPanguConfig.panguClusterName());
    workerConfMap.put(WorkerConfig.PY_CP_PANGU_ROOT_DIR,
        workerConfigTemplate.stateBackendPanguConfig.panguRootDir());
    workerConfMap.put(WorkerConfig.PY_CP_PANGU_USER_MYSQL_URL,
        workerConfigTemplate.stateBackendPanguConfig.panguUserMysqlUrl());
    workerConfMap.put(WorkerConfig.PY_METRICS_TYPE,
        workerConfigTemplate.metricConfig.metricType());
    workerConfMap.put(WorkerConfig.PY_METRICS_URL,
        workerConfigTemplate.metricPrometheusConfig.prometheusUrl());
    workerConfMap.put(WorkerConfig.PY_METRICS_USER_NAME,
        workerConfigTemplate.metricPrometheusConfig.prometheusUserName());
    workerConfMap.put(WorkerConfig.PY_RELIABILITY_LEVEL,
        workerConfigTemplate.reliabilityConfig.reliabilityLevel());
    workerConfMap.put(WorkerConfig.PY_QUEUE_TYPE,
        workerConfigTemplate.queueConfig.queueType());
    workerConfMap.put(WorkerConfig.PY_QUEUE_SIZE,
        workerConfigTemplate.queueConfig.queueSize() + "");
    workerConfMap.put(CommonConfig.PY_JOB_NAME,
        workerConfMap.get(WorkerConfig.JOB_NAME_INTERNAL));
    workerConfMap.put(WorkerConfig.PY_WORKER_ID,
        workerConfMap.get(WorkerConfig.WORKER_ID_INTERNAL));
  }

  /**
   * Get scheduled task details
   */
  public List<Map<String, String>> getSchedulerTaskInfo(ExecutionGraph executionGraph) {
    List<Map<String, String>> tasks = new ArrayList<>();

    List<ExecutionJobVertex> executionJobVertices = executionGraph.getVerticesInCreationOrder();
    int taskId = 1;

    for (ExecutionJobVertex jobVertex : executionJobVertices) {
      List<ExecutionVertex> executionVertices = jobVertex.getExeVertices();

      for (ExecutionVertex vertex : executionVertices) {
        Map<String, String> taskInfo = new HashMap<>();

        String originNodeId = jobVertex.getJobVertex().getName();
        if (originNodeId.contains(TASK_NODE_ID_SPILIT)) {
          taskInfo.put(TASK_NODE_ID, TASK_NODE_ID_PREFIX + originNodeId
              .substring(0, originNodeId.indexOf(TASK_NODE_ID_SPILIT)));
        } else {
          taskInfo.put(TASK_NODE_ID, TASK_NODE_ID_PREFIX + originNodeId);
        }
        taskInfo.put(TASK_INDEX, String.valueOf(vertex.getSubTaskIndex()));
        taskInfo.put(TASK_ID, String.valueOf(taskId++));
        taskInfo.put(TASK_ACTOR_ID, String.valueOf(vertex.getActor().getId()));

        tasks.add(taskInfo);
      }
    }

    LOG.info("scheduled task detail: {}", tasks);
    return tasks;
  }

}
