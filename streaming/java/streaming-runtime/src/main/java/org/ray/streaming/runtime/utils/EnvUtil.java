package org.ray.streaming.runtime.utils;

/**
 * 
 */
public class EnvUtil {

  private static final String ENV_KEY = "RAY_STREAMING_ENV";

  public static boolean isOnlineEnv() {
    String env = System.getenv(ENV_KEY);
    return env != null;
  }

}
