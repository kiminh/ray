package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.config.master.SchedulerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StreamingMasterConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingMasterConfig.class);

  public ResourceConfig resourceConfig;
  public SchedulerConfig schedulerConfig;

  public StreamingMasterConfig(Map<String, String> conf) {
    super(conf);
    resourceConfig = ConfigFactory.create(ResourceConfig.class, conf);
    schedulerConfig = ConfigFactory.create(SchedulerConfig.class, conf);

    configMap.putAll(masterConfig2Map());
  }

  @Override
  public String toString() {
    return configMap.toString();
  }

  public Map<String, String> masterConfig2Map() {
    Map<String, String> result = new HashMap<>();

    try {
      result.putAll(config2Map(this.resourceConfig));
      result.putAll(config2Map(this.schedulerConfig));
    } catch (Exception e) {
      LOGGER.error("Global config to map occur error.", e);
      return null;
    }

    return result;
  }
}
