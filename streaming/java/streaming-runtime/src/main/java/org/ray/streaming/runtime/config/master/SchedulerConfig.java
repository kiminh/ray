package org.ray.streaming.runtime.config.master;

import org.ray.streaming.runtime.config.Config;

/**
 * Job scheduler config.
 */
public interface SchedulerConfig extends Config {

  String SLOT_ASSIGN_STRATEGY = "streaming.scheduler.strategy.slot-assign";

  @DefaultValue(value = "pipeline_first_strategy")
  @Key(value = SLOT_ASSIGN_STRATEGY)
  String slotAssignStrategy();
}
