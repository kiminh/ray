package org.ray.runtime.context;

import java.nio.ByteBuffer;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskType;

/**
 * Worker context for cluster mode. This is a wrapper class for worker context of core worker.
 */
public class NativeWorkerContext implements WorkerContext {

  /**
   * The native pointer of core worker process.
   */
  private final long nativeCoreWorkerProcessPointer;

  private final ThreadLocal<ClassLoader> currentClassLoader = new ThreadLocal<>();

  public NativeWorkerContext(long nativeCoreWorkerProcessPointer) {
    this.nativeCoreWorkerProcessPointer = nativeCoreWorkerProcessPointer;
  }

  @Override
  public UniqueId getCurrentWorkerId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentWorkerId(nativeCoreWorkerProcessPointer));
  }

  @Override
  public JobId getCurrentJobId() {
    return JobId.fromByteBuffer(nativeGetCurrentJobId(nativeCoreWorkerProcessPointer));
  }

  @Override
  public ActorId getCurrentActorId() {
    return ActorId.fromByteBuffer(nativeGetCurrentActorId(nativeCoreWorkerProcessPointer));
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader.get();
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader.get() != currentClassLoader) {
      this.currentClassLoader.set(currentClassLoader);
    }
  }

  @Override
  public TaskType getCurrentTaskType() {
    return TaskType.forNumber(nativeGetCurrentTaskType(nativeCoreWorkerProcessPointer));
  }

  @Override
  public TaskId getCurrentTaskId() {
    return TaskId.fromByteBuffer(nativeGetCurrentTaskId(nativeCoreWorkerProcessPointer));
  }

  private static native int nativeGetCurrentTaskType(long nativeCoreWorkerProcessPointer);

  private static native ByteBuffer nativeGetCurrentTaskId(long nativeCoreWorkerProcessPointer);

  private static native ByteBuffer nativeGetCurrentJobId(long nativeCoreWorkerProcessPointer);

  private static native ByteBuffer nativeGetCurrentWorkerId(long nativeCoreWorkerProcessPointer);

  private static native ByteBuffer nativeGetCurrentActorId(long nativeCoreWorkerProcessPointer);
}
