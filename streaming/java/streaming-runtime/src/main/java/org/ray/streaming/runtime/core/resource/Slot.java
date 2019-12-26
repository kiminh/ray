package org.ray.streaming.runtime.core.resource;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;

public class Slot implements Serializable {
  private int id;
  private ContainerID containerID;
  private AtomicInteger actorCount = new AtomicInteger(0);

  public Slot(int id, ContainerID containerID) {
    this.id = id;
    this.containerID = containerID;
  }

  public int getId() {
    return id;
  }

  public ContainerID getContainerID() {
    return containerID;
  }

  public AtomicInteger getActorCount() {
    return actorCount;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("containerID", containerID)
        .add("actorCount", actorCount)
        .toString();
  }
}