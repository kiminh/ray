package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;

import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.worker.TransferConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.config.worker.WorkerConfig;

/**
 *
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public WorkerConfig workerConfig;

  public TransferConfig transferConfig;

  public StreamingWorkerConfig(Map<String, String> conf) {
    super(conf);
    workerConfig = ConfigFactory.create(WorkerConfig.class, conf);
    transferConfig = ConfigFactory.create(TransferConfig.class, conf);
    configMap.putAll(workerConfig2Map());
  }

  public Map<String, String> workerConfig2Map() {
    Map<String, String> result = new HashMap<>();
    try {
      result.putAll(config2Map(this.workerConfig));
      result.putAll(config2Map(this.transferConfig));
    } catch (Exception e) {
      LOGGER.error("Worker config to map occur error.", e);
      return null;
    }
    return result;
  }
}
