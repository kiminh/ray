package org.ray.streaming.runtime.config.types;

public enum OperatorType {

  /**
   * operator type
   */
  SOURCE(1),
  TRANSFORM(2),
  SINK(3),
  SOURCE_AND_SINK(4);

  private int value;

  OperatorType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
