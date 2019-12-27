package org.ray.streaming.runtime.master;

public enum JobMasterRuntimeStatus {

  /**
   * None
   */
  NONE("NONE", 0);

  private String name;
  private int index;

  JobMasterRuntimeStatus(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
