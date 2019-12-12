package org.ray.streaming.runtime.core.graph.jobgraph;

/**
 * A distribution pattern determines, which sub tasks of a producing task are connected to which
 * consuming sub tasks.
 */
public enum DistributionPattern {
  /**
   * Each producing sub task is connected to each sub task of the consuming task.
   */
  ALL_TO_ALL,
  /**
   * Each producing sub task is connected to one or more subtask(s) of the consuming task.
   */
  POINTWISE
}