package io.ray.runtime.actor;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.id.ActorId;

public abstract class BaseActorHandleImpl implements BaseActorHandle {

  /**
   * ID of the actor.
   */
  protected ActorId actorId;

  BaseActorHandleImpl(ActorId actorId) {
    this.actorId = actorId;
  }

  public ActorId getId() {
    return actorId;
  }

  @Override
  public void kill(boolean noRestart) {
    Ray.internal().killActor(this, noRestart);
  }

}
