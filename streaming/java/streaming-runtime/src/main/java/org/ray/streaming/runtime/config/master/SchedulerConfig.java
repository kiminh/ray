package org.ray.streaming.runtime.config.master;

import org.aeonbits.owner.Config;

/**
 *
 */
public interface SchedulerConfig extends Config {

  String SLOT_ASSIGN_STRATEGY = "streaming.scheduler.strategy.slot-assign";
  String RELEASE_IDLE_CONTAINER_ENABLE = "streaming.scheduler.strategy.release-idle-container.enable";
  String MAX_PARALLELISM = "streaming.scheduler.parallelism.max";

  @DefaultValue(value = "pipeline_first_strategy")
  @Key(value = SLOT_ASSIGN_STRATEGY)
  String slotAssignStrategy();

  @DefaultValue(value = "true")
  @Key(value = RELEASE_IDLE_CONTAINER_ENABLE)
  boolean releaseIdleContainerEnable();

  @DefaultValue(value = "10")
  @ConverterClass(MaxParallelismConverter.class)
  @Key(value = MAX_PARALLELISM)
  int maxParallelism();

  @DefaultValue(value = "10")
  int maxParallelismDev();

  @DefaultValue(value = "1024")
  int maxParallelismProd();
}
