package org.ray.streaming.runtime.master.scheduler;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * Streaming Runtime job scheduler interface
 */
public interface IJobScheduler {

  /**
   * Schedule streaming job
   *
   * @param executionGraph: The job's execution graph
   * @return true if scheduling job succeeded
   */
  boolean scheduleJob(ExecutionGraph executionGraph);
}
