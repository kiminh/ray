package org.ray.streaming.runtime.master.scheduler.controller;

import java.util.List;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;

/**
 * WorkerLifecycleController is responsible for JobWorker Actor's creation and destruction
 */
public interface IWorkerLifecycleController {

  /**
   * Create a worker actor
   * @param executionVertex: the specified execution vertex
   * @return true if worker creation succeeded
   */
  boolean createWorker(ExecutionVertex executionVertex);

  /**
   * Stop a worker actor
   * @param executionVertex: the specified execution vertex
   * @return true if worker destruction succeeded
   */
  boolean destroyWorker(ExecutionVertex executionVertex);

  /**
   * Create workers
   * @param executionVertices: the specified execution vertices
   * @return true if workers creation succeeded
   */
  boolean createWorkers(List<ExecutionVertex> executionVertices);

  /**
   * Create workers
   * @param executionVertices: the specified execution vertices
   * @return true if workers destruction succeeded
   */
  boolean destroyWorkers(List<ExecutionVertex> executionVertices);
}
