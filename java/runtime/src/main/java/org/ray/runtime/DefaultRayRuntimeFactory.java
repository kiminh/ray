package org.ray.runtime;

import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  private final Logger logger = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

  @Override
  public RayRuntime createRayRuntime(String config) {
    RayConfig rayConfig = RayConfig.create(config);
    try {
      AbstractRayRuntime runtime;
      if (rayConfig.runMode == RunMode.SINGLE_PROCESS) {
        runtime = new RayDevRuntime(rayConfig);
      } else {
        runtime = new RayNativeRuntime(rayConfig);
      }

      runtime.start();
      return runtime;
    } catch (Exception e) {
      logger.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
