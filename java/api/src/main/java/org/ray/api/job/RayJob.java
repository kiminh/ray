package org.ray.api.job;

import java.util.Optional;
import org.ray.api.RayObject;
import org.ray.api.id.JobId;

/**
 * Represents a sub-job created by `RayJobs.submitJob`.
 * @param <T>
 */
public interface RayJob <T> {

  /**
   * Get id of this job.
   */
  JobId getId();

  /**
   * Get the return value of job's root task.
   */
  Optional<RayObject<T>> getReturn();

  /**
   * Kill this job.
   */
  void kill();
}
