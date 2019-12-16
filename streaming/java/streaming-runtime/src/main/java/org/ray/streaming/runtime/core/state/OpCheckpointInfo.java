package org.ray.streaming.runtime.core.state;

import java.io.Serializable;
import java.util.Map;

public class OpCheckpointInfo implements Serializable {
  public Map<String, Object> inputPoints;
  public Map<String, Object> outputPoints;
  public long checkpointId;

  public OpCheckpointInfo() {
  }

  public OpCheckpointInfo(
      Map<String, Object> inputPoints,
      Map<String, Object> outputPoints,
      long checkpointId) {
    this.inputPoints = inputPoints;
    this.outputPoints = outputPoints;
    this.checkpointId = checkpointId;
  }
}