package org.ray.streaming.runtime.config.types;

public enum QueueType {

  /**
   * Memory type
   */
  MEMORY_QUEUE("memory_queue", 0),

  /**
   * Plasma type
   */
  PLASMA_QUEUE("plasma_queue", 1),

  /**
   * Streaming type
   */
  STREAMING_QUEUE("streaming_queue", 2);

  private String name;
  private int index;

  QueueType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
