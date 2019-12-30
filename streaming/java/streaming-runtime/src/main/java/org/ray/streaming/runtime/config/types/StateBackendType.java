package org.ray.streaming.runtime.config.types;

public enum StateBackendType {

  /**
   * Memory type
   */
  MEMORY("memory", 0),

  /**
   * Pangu type
   */
  PANGU("pangu", 1),

  /**
   * HBase type
   */
  HBASE("hbase", 2);

  private String value;
  private int index;

  StateBackendType(String value, int index) {
    this.value = value;
    this.index = index;
  }
}
