package org.ray.streaming.runtime.worker;

/**
 * The stream worker interface.
 */
public interface IJobWorker {


  void init(JobWorkerContext workerContext);

  /**
   *
   */
  void start();

  /**
   * Destroy worker
   */
  boolean destroy();

  /**
   * Shutdown worker in purpose
   */
  void shutdown();
}
