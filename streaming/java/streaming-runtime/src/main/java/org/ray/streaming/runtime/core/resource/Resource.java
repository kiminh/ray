package org.ray.streaming.runtime.core.resource;

import java.io.Serializable;
import java.util.List;

public class Resource implements Serializable {
  private final List<Container> occupiedContainers;
  private int totalCapacity;
  private int usedCapacity;

  public Resource(List<Container> occupiedContainers) {
    this.occupiedContainers = occupiedContainers;
  }
}