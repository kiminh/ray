package org.ray.api.runtime;

import org.ray.runtime.config.RayInitConfig;

/**
 * A factory that produces a RayRuntime instance.
 */
public interface RayRuntimeFactory {

  RayRuntime createRayRuntime(RayInitConfig config);
}
