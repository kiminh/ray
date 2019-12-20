package org.ray.streaming.runtime.worker;

import java.util.Map;

import com.google.common.base.MoreObjects;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.master.JobMaster;

/**
 *
 */
public class JobWorkerContext {

  private int taskId;
  private ActorId workerId;
  private ExecutionGraph executionGraph;
  private RayActor<JobMaster> master;
  private Map<String, String> conf;
  private Map<ActorId, String> inputActorQueues;
  private Map<ActorId, String> outputActorQueues;


  public JobWorkerContext(
      int taskId,
      ActorId workerId,
      Map<String, String> conf,
      ExecutionGraph executionGraph,
      RayActor<JobMaster> master,
      Map<ActorId, String> inputActorQueues,
      Map<ActorId, String> outputActorQueues) {
    this.taskId = taskId;
    this.workerId = workerId;
    this.conf = conf;
    this.executionGraph = executionGraph;
    this.master = master;
    this.inputActorQueues = inputActorQueues;
    this.outputActorQueues = outputActorQueues;
  }

  public int getTaskId() {
    return taskId;
  }

  public ActorId getWorkerId() {
    return workerId;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public RayActor<JobMaster> getMaster() {
    return master;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public Map<ActorId, String> getInputActorQueues() {
    return inputActorQueues;
  }

  public Map<ActorId, String> getOutputActorQueues() {
    return outputActorQueues;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("taskId", taskId)
        .add("workerId", workerId)
        .add("executionGraph", executionGraph)
        .add("master", master)
        .add("conf", conf)
        .add("inputActorQueues", inputActorQueues)
        .add("outputActorQueues", outputActorQueues)
        .toString();
  }
}
