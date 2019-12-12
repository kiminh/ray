package org.ray.streaming.runtime.config.master;

import org.aeonbits.owner.Config;

/**
 *
 */
public interface ResourceConfig extends Config {

  String CUSTOM_CONTAINER_CAPACITY = "streaming.resource.custom.container.capacity";

  @DefaultValue(value = "0")
  @Key(value = CUSTOM_CONTAINER_CAPACITY)
  int customContainerCapacity();
}
