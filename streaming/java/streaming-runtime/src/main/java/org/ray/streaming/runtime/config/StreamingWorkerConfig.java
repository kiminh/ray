package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.worker.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public WorkerConfig workerConfig;

  public StreamingWorkerConfig(Map<String, String> conf) {
    super(conf);
    workerConfig = ConfigFactory.create(WorkerConfig.class, conf);
    configMap.putAll(workerConfig2Map());
  }

  public Map<String, String> workerConfig2Map() {
    Map<String, String> result = new HashMap<>();
    try {
      result.putAll(config2Map(this.workerConfig));
    } catch (Exception e) {
      LOGGER.error("Worker config to map occur error.", e);
      return null;
    }
    return result;
  }
}
