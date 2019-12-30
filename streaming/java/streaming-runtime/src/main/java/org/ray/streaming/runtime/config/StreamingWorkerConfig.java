package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;

import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import org.ray.streaming.runtime.util.LoggerFactory;

/**
 * Streaming job worker specified config.
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public WorkerInternalConfig workerInternalConfig;

  public StreamingWorkerConfig(Map<String, String> conf) {
    super(conf);
    workerInternalConfig = ConfigFactory.create(WorkerInternalConfig.class, conf);

    configMap.putAll(workerConfig2Map());
  }

  public Map<String, String> workerConfig2Map() {
    Map<String, String> result = new HashMap<>();
    try {
      result.putAll(config2Map(this.workerInternalConfig));
    } catch (Exception e) {
      LOGGER.error("Worker config to map occur error.", e);
      return null;
    }
    return result;
  }
}
