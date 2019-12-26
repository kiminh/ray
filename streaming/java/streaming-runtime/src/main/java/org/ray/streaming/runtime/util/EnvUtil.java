package org.ray.streaming.runtime.util;

import java.lang.management.ManagementFactory;

import org.ray.runtime.RayNativeRuntime;

/**
 *
 */
public class EnvUtil {

  private static final String ENV_KEY = "RAY_STREAMING_ENV";

  public static boolean isOnlineEnv() {
    String env = System.getenv(ENV_KEY);
    return env != null;
  }

  public static String getJvmPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public static void loadNativeLibraries() {
    // Explicitly load `RayNativeRuntime`, to make sure `core_worker_library_java`
    // is loaded before `streaming_java`.
    try {
      Class.forName(RayNativeRuntime.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    JniUtils.loadLibrary("streaming_java");
  }
}
