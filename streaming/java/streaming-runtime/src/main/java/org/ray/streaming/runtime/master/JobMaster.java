package org.ray.streaming.runtime.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.config.global.StateBackendConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraphBuilder;
import org.ray.streaming.runtime.core.graph.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.state.StateBackend;
import org.ray.streaming.runtime.core.state.StateBackendFactory;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import org.ray.streaming.runtime.master.scheduler.JobScheduler;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.util.ModuleNameAppender;
import org.ray.streaming.runtime.util.TestHelper;

@RayRemote
public class JobMaster implements IJobMaster {

  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

  private JobMasterRuntimeContext runtimeContext;
  private Map<ActorId, RayActor> sourceActors;
  private Map<ActorId, RayActor> sinkActors;
  private StateBackend<String, byte[], StateBackendConfig> stateBackend;
  private StreamingMasterConfig conf;
  private ResourceManager resourceManager;
  private JobScheduler scheduler;
  private GraphManager graphManager;
  private WorkerLifecycleController workerController;
  private RayActor jobMasterActor;
  private Executor executor;
  private int updateOperatorIndex = 0;

  // For test
  public static JobMaster jobMaster;

  public JobMaster(Map<String, String> confMap) {
    if (TestHelper.isUTPattern()) {
      jobMaster = this;
    }
    LOG.info("Job master conf is {}.", confMap);

    StreamingConfig streamingConfig = new StreamingConfig(confMap);
    this.conf = streamingConfig.masterConfig;

    // init state backend
    stateBackend = StateBackendFactory.getStateBackend(conf);

    // init runtime context
    runtimeContext = new JobMasterRuntimeContext(streamingConfig);

    String moduleName = conf.commonConfig.jobName();
    ModuleNameAppender.setModuleName(moduleName);

    executor = Executors.newFixedThreadPool(1);
    LOG.info("Job master init success");
  }

  @Override
  public Boolean init(boolean isRecover) {
    LOG.info("Begin register job master context. Is recover: {}.", isRecover);

    if (this.runtimeContext.getGraphs() == null) {
      LOG.error("Register job master context failed. Job graphs is null.");
      return false;
    }

    // recover from last checkpoint
    if (isRecover) {
      LOG.info("Recover graph manager, resource manager and scheduler.");
      graphManager = new GraphManagerImpl(this);
      resourceManager = new ResourceManagerImpl(this);
      scheduler = new JobScheduler(this);
    }

    workerController = new WorkerLifecycleController(resourceManager);

    ExecutionGraph executionGraph = graphManager.getExecutionGraph();
    Preconditions.checkArgument(executionGraph != null, "no execution graph");

    this.sourceActors = executionGraph.getSourceActorsMap();
    this.sinkActors = executionGraph.getSinkActorsMap();
    Preconditions.checkArgument(!sourceActors.isEmpty(), "no sourceActor");
    Preconditions.checkArgument(!sinkActors.isEmpty(), "no sinkActor");

    LOG.info("Finish register job master context.");
    return true;
  }



  public boolean submitJob(RayActor jobMasterActor, byte[] jobGraphByteArray) {
    JobGraph jobGraph = KryoUtils.readFromByteArray(jobGraphByteArray);
    LOG.info("Job vertices num is: {}.", jobGraph.getVertices().size());
    this.jobMasterActor = jobMasterActor;
    ExecutionGraph executionGraph = ExecutionGraphBuilder.buildGraph(jobGraph);

    // set init graphs into runtime context
    runtimeContext.setGraphs(jobGraph, executionGraph);

    // init manager
    graphManager = new GraphManagerImpl(this);
    resourceManager = new ResourceManagerImpl(this);

    scheduler = new JobScheduler(this);
    scheduler.scheduleJob(executionGraph);
    return true;
  }

  /**
   * Start all workers.
   */
  @Override
  public void startAllWorkers() {
    LOG.info("Start to start all workers.");
    long startWaitTs = System.currentTimeMillis();

    try {
      startWorkersByList(graphManager.getExecutionGraph().getAllActors());
    } catch (Exception e) {
      LOG.error("Failed to start all workers.", e);
    }

    LOG.info("Finish to start all workers, cost {} ms.", System.currentTimeMillis() - startWaitTs);
  }

  /**
   * Start workers by actor list.
   * @param addedActors actor list
   */
  private void startWorkersByList(List<RayActor> addedActors) {
    ExecutionGraph executionGraph = graphManager.getExecutionGraph();

    executionGraph.getSourceActors()
        .stream()
        .filter(addedActors::contains)
        .forEach(actor -> workerController.startWorker(actor));

    executionGraph.getNonSourceActors()
        .stream()
        .filter(addedActors::contains)
        .forEach(actor -> workerController.startWorker(actor));
  }

  @Override
  public Boolean destroyAllWorkers() {
    graphManager.getExecutionGraph().getAllExecutionVertices()
        .forEach(vertex -> workerController.destroyWorker(vertex));
    return true;
  }

  public RayActor getJobMasterActor() {
    return jobMasterActor;
  }

  public JobMasterRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  public GraphManager getGraphManager() {
    return graphManager;
  }

  public StateBackend<String, byte[], StateBackendConfig> getStateBackend() {
    return stateBackend;
  }

  public StreamingMasterConfig getConf() {
    return conf;
  }

  private String getActorName(ActorId id) {
    return graphManager.getExecutionGraph().getActorName(id);
  }
}
