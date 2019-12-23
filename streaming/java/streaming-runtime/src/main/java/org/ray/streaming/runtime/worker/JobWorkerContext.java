package org.ray.streaming.runtime.worker;

import com.google.common.base.MoreObjects;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;

import org.ray.streaming.runtime.master.JobMaster;

/**
 *
 */
public class JobWorkerContext {

  private ActorId workerId;
  private RayActor<JobMaster> master;
  private byte[] executionVertexBytes;


  public JobWorkerContext(
      ActorId workerId,
      RayActor<JobMaster> master,
      byte[] executionVertexBytes) {
    this.workerId = workerId;
    this.master = master;
    this.executionVertexBytes = executionVertexBytes;
  }

  public ActorId getWorkerId() {
    return workerId;
  }

  public RayActor<JobMaster> getMaster() {
    return master;
  }

  public byte[] getExecutionVertexBytes() {
    return executionVertexBytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerId", workerId)
        .add("master", master)
        .toString();
  }
}
