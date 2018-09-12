package org.ray.api.runtime;

import org.ray.runtime.config.RayInitConfig;

import java.lang.reflect.Method;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  //TODO(qwang): We just create a RayRuntime object and return it.
  // We should not invoke `AbstractRayRuntime.init()`.
  // Or when we create it , we call `init()` as soon as.
  @Override
  public RayRuntime createRayRuntime(RayInitConfig initConfig) {
    try {
      Class cls = Class.forName("org.ray.runtime.AbstractRayRuntime");
      RayRuntime runtime = (RayRuntime) cls.newInstance();

      Method init = cls.getDeclaredMethod("init");
      init.setAccessible(true);
      init.invoke(runtime, initConfig);
      init.setAccessible(false);
      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
