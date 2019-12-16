package org.ray.streaming.runtime.config.types;

public enum CheckpointModeType {

  /**
   * Sync type
   */
  SYNC("sync", 0),

  /**
   * Async type
   */
  ASYNC("async", 1);

  private String name;
  private int index;

  CheckpointModeType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
