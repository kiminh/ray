package io.ray.runtime.task;

import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.id.ActorId;
import io.ray.runtime.id.UniqueId;

/**
 * Task executor for local mode.
 */
public class LocalModeTaskExecutor extends TaskExecutor<LocalModeTaskExecutor.LocalActorContext> {

  static class LocalActorContext extends TaskExecutor.ActorContext {

    /**
     * The worker ID of the actor.
     */
    private final UniqueId workerId;

    public LocalActorContext(UniqueId workerId) {
      this.workerId = workerId;
    }

    public UniqueId getWorkerId() {
      return workerId;
    }
  }

  public LocalModeTaskExecutor(RayRuntimeInternal runtime) {
    super(runtime);
  }

  @Override
  protected LocalActorContext createActorContext() {
    return new LocalActorContext(runtime.getWorkerContext().getCurrentWorkerId());
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
  }

  @Override
  protected void maybeLoadCheckpoint(Object actor, ActorId actorId) {
  }
}
