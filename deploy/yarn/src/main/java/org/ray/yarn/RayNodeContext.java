package org.ray.yarn;

import org.apache.hadoop.yarn.api.records.Container;

public class RayNodeContext {

  RayNodeContext(String role) {
    this.role = role;
  }

  public RayNodeContext(String role, Long requestId) {
    this.role = role;
    this.requestId = requestId;
  }

  String role;

  boolean isRunning = false;

  boolean isAllocating = false;

  boolean isReleasing = false;

  boolean isCompleted = false;

  String instanceId = null;

  Container container = null;

  int failCounter = 0;

  Long requestId;
}
