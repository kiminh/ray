package org.ray.runner.worker;

import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.core.model.WorkerMode;
import org.ray.runner.JobUpdater;
import org.ray.spi.impl.ray.protocol.JobState;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.impl.RedisClient;


/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  //
  public static void main(String[] args) {
    KeyValueStoreLink kvStore = null;
    UniqueID jobId = UniqueID.nil;

    try{
      RayRuntime.init(args);
      assert RayRuntime.getParams().worker_mode == WorkerMode.DRIVER;
      kvStore = new RedisClient();
      kvStore.setAddr(RayRuntime.getParams().redis_address);
      jobId = RayRuntime.getParams().driver_id;
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      JobUpdater.setJobState(kvStore, jobId, JobState.Started);
      String driverClass = RayRuntime.configReader
          .getStringValue("ray.java.start", "driver_class", "",
              "java class which main is served as the driver in a java worker");
      String driverArgs = RayRuntime.configReader
          .getStringValue("ray.java.start", "driver_args", "",
              "arguments for the java class main function which is served at the driver");
      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",") : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
      JobUpdater.setJobState(kvStore, jobId, JobState.Completed);
    } catch (Throwable e) {
      JobUpdater.setJobState(kvStore, jobId, JobState.Failed);
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
