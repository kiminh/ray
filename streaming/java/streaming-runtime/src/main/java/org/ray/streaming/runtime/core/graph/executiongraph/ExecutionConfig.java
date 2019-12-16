package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

import org.ray.streaming.runtime.config.Configuration;
import org.ray.streaming.runtime.config.internal.WorkerConfig;

public class ExecutionConfig implements Serializable {

  private static final String WORKER_PARALLELISM_INDEX = "worker_parallelism_index";

  private final Configuration configuration;

  public ExecutionConfig(Configuration configuration) {
    this.configuration = configuration;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setSubTaskIndex(int subTaskIndex) {
    configuration.setInteger(WORKER_PARALLELISM_INDEX, subTaskIndex);
  }

  public int getSubTaskIndex() {
    return configuration.getInteger(WORKER_PARALLELISM_INDEX, 0);
  }

  public void setWorkerId(String id) {
    configuration.setString(WorkerConfig.WORKER_ID_INTERNAL, id);
  }

  public String getWorkerId() {
    return configuration.getString(WorkerConfig.WORKER_ID_INTERNAL, "");
  }
}