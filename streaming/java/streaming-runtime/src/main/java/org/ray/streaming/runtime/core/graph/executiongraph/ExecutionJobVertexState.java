package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

public enum ExecutionJobVertexState implements Serializable {
  NORMAL("normal vertex"),
  CHANGED("rescaling changed vertex"),
  AFFECTED_UP_STREAM("rescaling affected up stream vertex"),
  AFFECTED_DOWN_STREAM("rescaling affected up stream vertex"),
  AFFECTED_NEIGHBOUR("rescaling affected neighbour"),
  AFFECTED_NEIGHBOUR_PARENT("rescaling affected neighbour parent");

  private String value;

  ExecutionJobVertexState(String value) {
    this.value = value;
  }
}
