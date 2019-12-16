package org.ray.streaming.runtime.worker;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import com.google.common.base.MoreObjects;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.streaming.runtime.config.types.OperatorType;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.worker.task.ControlMessage;

/**
 *
 */
public class JobWorkerContext {

  public final String jobName;
  public final String opName;
  public final String workerName;
  public RayActor<JobMaster> master;
  public final ActorId workerId;
  public final Map<String, String> conf;
  public final Map<ActorId, String> inputActorQueues;
  public final Map<ActorId, String> outputActorQueues;
  public final Map<String, RayActor> inputActors;
  public final Map<String, RayActor> outputActors;
  public UniqueId rayCheckpointId;
  // the role(source/transform/sink) of current worker in changed sub dag
  public OperatorType roleInChangedSubDag = OperatorType.TRANSFORM;
  // control messages
  public ArrayBlockingQueue<ControlMessage> mailbox = new ArrayBlockingQueue(16);

  /** The execution vertex info */
  public byte[] executionVertexBytes;

  public JobWorkerContext(
      String jobName,
      String opName,
      String workerName,
      RayActor<JobMaster> master,
      ActorId workerId,
      Map<String, String> conf,
      Map<ActorId, String> inputQueues,
      Map<ActorId, String> outputQueues,
      Map<String, RayActor> inputActors,
      Map<String, RayActor> outputActors) {
    this(jobName, opName, workerName, master, workerId, conf, inputQueues,
        outputQueues, inputActors, outputActors, null);
  }

  public JobWorkerContext(
      String jobName,
      String opName,
      String workerName,
      RayActor<JobMaster> master,
      ActorId workerId,
      Map<String, String> conf,
      Map<ActorId, String> inputQueues,
      Map<ActorId, String> outputQueues,
      Map<String, RayActor> inputActors,
      Map<String, RayActor> outputActors,
      byte[] executionVertexBytes) {
    this.jobName = jobName;
    this.opName = opName;
    this.workerName = workerName;
    this.master = master;
    this.workerId = workerId;
    this.conf = Collections.unmodifiableMap(conf);
    this.inputActorQueues = Collections.unmodifiableMap(inputQueues);
    this.outputActorQueues = Collections.unmodifiableMap(outputQueues);
    this.inputActors = Collections.unmodifiableMap(inputActors);
    this.outputActors = Collections.unmodifiableMap(outputActors);
    this.executionVertexBytes = executionVertexBytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobName", jobName)
        .add("opName", opName)
        .add("workerName", workerName)
        .add("master", master)
        .add("workerId", workerId)
        .add("conf", conf)
        .add("inputActorQueues", inputActorQueues)
        .add("outputActorQueues", outputActorQueues)
        .add("rayCheckpointId", rayCheckpointId)
        .add("isChanged", isChanged)
        .add("roleInChangedSubDag", roleInChangedSubDag)
        .add("mailbox", mailbox)
        .add("rollbackTriggerId", rollbackTriggerId)
        .add("isRollbackTriggerValid", isRollbackTriggerValid)
        .add("inputActors", inputActors)
        .add("outputActors", outputActors)
        .toString();
  }

  public void markAsChanged() {
    this.isChanged = true;
  }

}
