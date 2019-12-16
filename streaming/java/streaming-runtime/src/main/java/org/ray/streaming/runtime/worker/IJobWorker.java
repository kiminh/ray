package org.ray.streaming.runtime.worker;

/**
 * The stream worker interface.
 */
public interface IJobWorker {

  /**
   * Init job worker with context.
   * @param workerContext
   * @return Init result.
   */
  Boolean init(JobWorkerContext workerContext);

  /**
   * Start job worker working progress.
   */
  void start();

  /**
   * Destroy worker.
   */
  Boolean destroy();

  /**
   * Shutdown worker in purpose.
   */
  void shutdown();
}
