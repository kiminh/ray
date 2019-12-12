package org.ray.streaming.runtime.config.types;

public enum SlotAssignStrategyType {

  /**
   * PIPELINE_FIRST_STRATEGY
   */
  PIPELINE_FIRST_STRATEGY("pipeline_first_strategy", 0),

  /**
   * COLOCATE_STRATEGY
   */
  COLOCATE_STRATEGY("colocate_strategy", 1),

  /**
   * JOB_VERTEX_SPLIT_STRATEGY
   */
  JOB_VERTEX_SPLIT_STRATEGY("job_vertex_split_strategy", 2),

  /**
   * STABLE_SHIFT_STRATEGY
   */
  STABLE_SHIFT_STRATEGY("stable_shift_strategy", 3);

  private String name;
  private int index;

  SlotAssignStrategyType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
