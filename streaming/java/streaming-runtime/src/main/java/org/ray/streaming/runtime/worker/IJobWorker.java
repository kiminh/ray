package org.ray.streaming.runtime.worker;

import java.util.Collection;

/**
 * The stream worker interface.
 */
public interface IJobWorker {

  /**
   * Register job worker context
   * @param workerContext job worker context
   */
  void registerContext(JobWorkerContext workerContext);

  void start();

  /**
   * Check if this worker needs rollback.
   * @return if need rollback, return true.
   */
  Boolean checkNeedRollback();

  /**
   * Update job worker context.
   * @param workerContext
   */
  void updateContext(JobWorkerContext workerContext);

  /**
   * Check if rollback trigger exist according to the trigger id value (>0 = true)
   * @return
   */
  Boolean checkRollbackTriggerExist();


  void shutdownWithoutReconstruction();

  /**
   * Destroy worker
   */
  boolean destroy();

  /**
   * Shutdown worker in purpose
   */
  void shutdown();
}
