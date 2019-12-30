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

  private String value;
  private int index;

  SlotAssignStrategyType(String value, int index) {
    this.value = value;
    this.index = index;
  }

  public String getValue() {
    return value;
  }
}
