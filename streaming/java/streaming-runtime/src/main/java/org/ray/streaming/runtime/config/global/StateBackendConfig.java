package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Job state backend config.
 */
public interface StateBackendConfig extends Config {

  String STATE_BACKEND_TYPE = "streaming.state-backend.type";

  @DefaultValue(value = "memory")
  @Key(value = STATE_BACKEND_TYPE)
  String stateBackendType();

}
