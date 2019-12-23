package org.ray.streaming.jobgraph;

/**
 * Different roles for a node.
 */
public enum VertexType {

  /**
   * Source type.
   */
  SOURCE("source", 1),

  /**
   * Process type.
   */
  PROCESS("process", 2),

  /**
   * Sink type.
   */
  SINK("sink", 3);

  private String name;
  private int index;

  VertexType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
