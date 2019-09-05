package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.NativeWorkerContext;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.GcsClientOptions;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.object.NativeObjectStore;
import org.ray.runtime.raylet.NativeRayletClient;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.task.NativeTaskSubmitter;
import org.ray.runtime.task.TaskExecutor;
import org.ray.runtime.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

  /**
   * The native pointer of core worker process.
   */
  private long nativeCoreWorkerProcessPointer;

  static {
    LOGGER.debug("Loading native libraries.");
    // Load native libraries.
    String[] libraries = new String[]{"core_worker_library_java"};
    for (String library : libraries) {
      String fileName = System.mapLibraryName(library);
      try (FileUtil.TempFile libFile = FileUtil.getTempFileFromResource(fileName)) {
        System.load(libFile.getFile().getAbsolutePath());
      }
      LOGGER.debug("Native libraries loaded.");
    }

    RayConfig globalRayConfig = RayConfig.create();
    // Reset library path at runtime.
    resetLibraryPath(globalRayConfig);

    try {
      FileUtils.forceMkdir(new File(globalRayConfig.logDir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create the log directory.", e);
    }
    nativeSetup(globalRayConfig.logDir);
    Runtime.getRuntime().addShutdownHook(new Thread(RayNativeRuntime::nativeShutdownHook));
  }

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private static void resetLibraryPath(RayConfig rayConfig) {
    if (rayConfig.libraryPath.isEmpty()) {
      return;
    }

    String path = System.getProperty("java.library.path");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += ":";
    }
    path += String.join(":", rayConfig.libraryPath);

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    // Set sys_paths to null so that java.library.path will be re-evaluated next time it is needed.
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOGGER.error("Failed to set library path.", e);
    }
  }

  @Override
  public void start() {
    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }

    gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);

    if (rayConfig.getJobId() == JobId.NIL) {
      rayConfig.setJobId(gcsClient.nextJobId());
    }
    // TODO(qwang): Get object_store_socket_name and raylet_socket_name from Redis.
    nativeCoreWorkerProcessPointer = nativeInitCoreWorkerProcess(rayConfig.workerMode.getNumber(),
        getStaticWorkerInfo(),
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName,
        (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
        new GcsClientOptions(rayConfig));
    Preconditions.checkState(nativeCoreWorkerProcessPointer != 0);

    taskExecutor = new TaskExecutor(this);
    workerContext = new NativeWorkerContext(nativeCoreWorkerProcessPointer);
    objectStore = new NativeObjectStore(workerContext, nativeCoreWorkerProcessPointer);
    taskSubmitter = new NativeTaskSubmitter(nativeCoreWorkerProcessPointer);
    rayletClient = new NativeRayletClient(nativeCoreWorkerProcessPointer);

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (nativeCoreWorkerProcessPointer != 0) {
      nativeDestroyCoreWorkerProcess(nativeCoreWorkerProcessPointer);
      nativeCoreWorkerProcessPointer = 0;
    }
    if (null != manager) {
      manager.cleanup();
    }
  }

  @Override
  public Runnable asyncClosure(Runnable runnable) {
    UniqueId workerId = workerContext.getCurrentWorkerId();
    return () -> {
      nativeSetCoreWorker(nativeCoreWorkerProcessPointer, workerId.getBytes());
      runnable.run();
    };
  }

  @Override
  public Callable asyncClosure(Callable callable) {
    UniqueId workerId = workerContext.getCurrentWorkerId();
    return () -> {
      nativeSetCoreWorker(nativeCoreWorkerProcessPointer, workerId.getBytes());
      return callable.call();
    };
  }

  public void run() {
    nativeRunTaskExecutor(nativeCoreWorkerProcessPointer, taskExecutor);
  }

  private Map<String, String> getStaticWorkerInfo() {
    Map<String, String> workerInfo = new HashMap<>();
    workerInfo.put("node_ip_address", rayConfig.nodeIp);
    workerInfo.put("name", System.getProperty("user.dir"));
    return workerInfo;
  }

  private static native long nativeInitCoreWorkerProcess(int workerMode,
      Map<String, String> staticWorkerInfo, String storeSocket,
      String rayletSocket, byte[] jobId, GcsClientOptions gcsClientOptions);

  private static native void nativeRunTaskExecutor(long nativeCoreWorkerProcessPointer,
      TaskExecutor taskExecutor);

  private static native void nativeDestroyCoreWorkerProcess(long nativeCoreWorkerProcessPointer);

  private static native void nativeSetup(String logDir);

  private static native void nativeShutdownHook();

  private static native void nativeSetCoreWorker(long nativeCoreWorkerProcessPointer,
      byte[] workerId);
}
