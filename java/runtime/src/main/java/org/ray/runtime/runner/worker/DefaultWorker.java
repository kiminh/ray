package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RayInitConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;

/**
 * default worker implementation.
 */
public class DefaultWorker {

  //
  // String workerCmd = "java" + " -jarls " + workerPath + " --node-ip-address=" + ip
  // + " --object-store-name=" + storeName
  // + " --object-store-manager-name=" + storeManagerName
  // + " --local-scheduler-name=" + name + " --redis-address=" + redisAddress
  //
  public static void main(String[] args) {
    try {
      RayInitConfig config = new RayInitConfig();

      for (String arg: args) {
        if (arg.startsWith("--redis-address=")) {
          config.setRedisIpAddr(arg.substring("--redis-address=".length()));
        } else if (arg.startsWith("--node-ip-address=")) {
          config.setProperity("nodeIpAddr", arg.substring("--redis-address=".length()));
        } else {
          // TODO(qwang): We should throw this.
          // throw RuntimeException("Unreconginazed command line args.");
        }
      }

      //TODO(qwang): We should read the run mode from env variable or user define.
      // I think this should be in the command line args.
      config.setRunMode(RunMode.SINGLE_BOX);
      config.setWorkerMode(WorkerMode.WORKER);
      Ray.init(config);

      // TODO(qwang): We should redesign the interface to make below line better.
      ((AbstractRayRuntime)Ray.internal()).loop();
      throw new RuntimeException("Control flow should never reach here");

    } catch (Throwable e) {
      e.printStackTrace();
      System.err
          .println("--config=ray.config.ini --overwrite=ray.java.start.worker_mode=WORKER;...");
      System.exit(-1);
    }
  }
}
