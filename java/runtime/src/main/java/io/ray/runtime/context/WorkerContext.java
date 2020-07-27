package io.ray.runtime.context;

import io.ray.runtime.generated.Common.TaskType;
import io.ray.runtime.id.ActorId;
import io.ray.runtime.id.JobId;
import io.ray.runtime.id.TaskId;
import io.ray.runtime.id.UniqueId;

/**
 * The context of worker.
 */
public interface WorkerContext {

  /**
   * ID of the current worker.
   */
  UniqueId getCurrentWorkerId();

  /**
   * ID of the current job.
   */
  JobId getCurrentJobId();

  /**
   * ID of the current actor.
   */
  ActorId getCurrentActorId();

  /**
   * The class loader that is associated with the current job. It's used for locating classes when
   * dealing with serialization and deserialization in {@link Serializer}.
   */
  ClassLoader getCurrentClassLoader();

  /**
   * Set the current class loader.
   */
  void setCurrentClassLoader(ClassLoader currentClassLoader);

  /**
   * Type of the current task.
   */
  TaskType getCurrentTaskType();

  /**
   * ID of the current task.
   */
  TaskId getCurrentTaskId();
}
