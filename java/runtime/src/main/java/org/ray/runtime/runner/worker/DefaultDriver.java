package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.runtime.config.RayInitConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  // + " --class-args" + classArgs
  //
  public static void main(String[] args) {
    try {
      RayInitConfig config = new RayInitConfig();

      for (String arg : args) {
        if (arg.startsWith("--redis-address=")) {
          config.setRedisAddr(arg.substring("--redis-address=".length()));
        } else if (arg.startsWith("--node-ip-address=")) {
          config.setProperity("nodeIpAddr", arg.substring("--node-ip-address=".length()));
        } else if (arg.startsWith("--driver-class")) {
          config.setProperity("driverClass", arg.substring("--node-ip-address=".length()));
        } else if (arg.startsWith("--class-args")) {
          config.setProperity("classArgs", arg.substring("--class-args".length()));
        } else {
          // TODO(qwang): We should throw this.
          // throw RuntimeException("Unreconginazed command line args.");
        }
      }

      //TODO(qwang): We should read the run mode from env variable or user define.
      // I think this should be in the command line args.
      config.setRunMode(RunMode.SINGLE_BOX);
      config.setWorkerMode(WorkerMode.DRIVER);
      Ray.init(config);


      String driverClass = config.getProperity("driverClass");
      String driverArgs = config.getProperity("classArgs");
      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",") : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
