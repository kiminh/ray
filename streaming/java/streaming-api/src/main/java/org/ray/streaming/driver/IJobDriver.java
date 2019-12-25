package org.ray.streaming.driver;

import java.util.Map;

import org.ray.streaming.jobgraph.JobGraph;

/**
 * Interface of the job driver.
 */
public interface IJobDriver {

  /**
   * Assign logical plan to physical execution graph, and submit job to run.
   *
   * @param jobGraph The logical plan.
   */
  void submit(JobGraph jobGraph, Map<String, String> conf);
}
