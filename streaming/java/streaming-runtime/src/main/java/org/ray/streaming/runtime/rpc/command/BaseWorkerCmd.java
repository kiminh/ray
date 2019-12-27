package org.ray.streaming.runtime.rpc.command;

import java.io.Serializable;

import org.ray.api.id.ActorId;

/**
 * Base command that worker use to call master.
 */
public abstract class BaseWorkerCmd implements Serializable {

  public ActorId fromActorId;

  public BaseWorkerCmd() {
  }

  protected BaseWorkerCmd(ActorId actorId) {
    this.fromActorId = actorId;
  }
}