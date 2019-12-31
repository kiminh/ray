package org.ray.streaming.runtime.master;

import java.util.Map;

import com.google.common.base.Preconditions;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.streaming.jobgraph.JobGraph;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.config.global.StateBackendConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
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

    LOG.info("Job master init success");
  }

  @Override
  public Boolean init(boolean isRecover) {
    LOG.info("Start to init job master. Is recover: {}.", isRecover);

    if (this.runtimeContext.getGraphs() == null) {
      LOG.error("Init job master failed. Job graphs is null.");
      return false;
    }

    // recover from last checkpoint
    if (isRecover) {
      LOG.info("Recover graph manager, resource manager and scheduler.");
      graphManager = new GraphManagerImpl(this);
      resourceManager = new ResourceManagerImpl(this);
      resourceManager.setResources(runtimeContext.getResources());
      scheduler = new JobScheduler(this);
    }

    workerController = new WorkerLifecycleController();

    ExecutionGraph executionGraph = graphManager.getExecutionGraph();
    Preconditions.checkArgument(executionGraph != null, "no execution graph");

    this.sourceActors = executionGraph.getSourceActorsMap();
    this.sinkActors = executionGraph.getSinkActorsMap();
    Preconditions.checkArgument(!sourceActors.isEmpty(), "no sourceActor");
    Preconditions.checkArgument(!sinkActors.isEmpty(), "no sinkActor");

    LOG.info("Finish to init job master.");
    return true;
  }

  public boolean submitJob(RayActor jobMasterActor, byte[] jobGraphByteArray) {
    JobGraph jobGraph = KryoUtils.readFromByteArray(jobGraphByteArray);
    LOG.info("Job vertices num is: {}.", jobGraph.getJobVertexList().size());

    this.jobMasterActor = jobMasterActor;

    // init manager
    graphManager = new GraphManagerImpl(this);
    resourceManager = new ResourceManagerImpl(this);
    runtimeContext.setResources(resourceManager.getResources());

    // build and set graph into runtime context
    ExecutionGraph executionGraph = graphManager.buildExecutionGraph(jobGraph);
    runtimeContext.setGraphs(jobGraph, executionGraph);

    // init scheduler
    scheduler = new JobScheduler(this);
    scheduler.scheduleJob(graphManager.getExecutionGraph());
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

  public WorkerLifecycleController getWorkerController() {
    return workerController;
  }

  public StateBackend<String, byte[], StateBackendConfig> getStateBackend() {
    return stateBackend;
  }

  public StreamingMasterConfig getConf() {
    return conf;
  }
}
