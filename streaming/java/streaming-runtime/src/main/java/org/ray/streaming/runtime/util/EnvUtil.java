package org.ray.streaming.runtime.util;

import java.lang.management.ManagementFactory;

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
}
