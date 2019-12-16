package org.ray.streaming.runtime.core.resource;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class Slot implements Serializable {
  private int id;
  private Container container;
  private AtomicInteger actorCount = new AtomicInteger(0);

  public Slot(int id, Container container) {
    this.id = id;
    this.container = container;
  }

  public int getId() {
    return id;
  }

  public Container getContainer() {
    return container;
  }

  public AtomicInteger getActorCount() {
    return actorCount;
  }

  @Override
  public String toString() {
    return String.format("{id = %d, Container = %s, actorCount = %d}", id, container, actorCount.get());
  }
}