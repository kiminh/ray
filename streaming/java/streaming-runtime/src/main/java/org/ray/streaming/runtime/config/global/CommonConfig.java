package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Job common config.
 */
public interface CommonConfig extends Config {

  String JOB_NAME = "streaming.job.name";

  /**
   * Streaming job name.
   * @return
   */
  @DefaultValue(value = "default-job-name")
  @Key(value = JOB_NAME)
  String jobName();
}
