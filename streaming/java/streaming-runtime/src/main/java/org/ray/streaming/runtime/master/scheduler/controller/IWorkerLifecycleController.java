package org.ray.streaming.runtime.master.scheduler.controller;

import java.util.Map;

import org.ray.api.RayActor;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;

/**
 * WorkerLifecycleController is responsible for JobWorker Actor's life cycle.
 */
public interface IWorkerLifecycleController {

  /**
   * Create a worker.
   * @param executionVertex: the specified execution vertex
   * @param resources: resources allocate by resource manager
   * @return true if worker creation succeed
   */
  boolean createWorker(ExecutionVertex executionVertex, Map<String, Double> resources);

  /**
   * Init a worker.
   * @param rayActor: target worker's actor
   * @param jobWorkerContext: the worker context
   * @return true if worker initiation succeed
   */
  boolean initWorker(RayActor rayActor, JobWorkerContext jobWorkerContext);

  /**
   * Start a worker.
   * @param rayActor: target worker's actor
   * @return true if worker starting succeed
   */
  boolean startWorker(RayActor rayActor);

  /**
   * Stop a worker.
   * @param executionVertex: the specified execution vertex
   * @return true if worker destruction succeed
   */
  boolean destroyWorker(ExecutionVertex executionVertex);
}
