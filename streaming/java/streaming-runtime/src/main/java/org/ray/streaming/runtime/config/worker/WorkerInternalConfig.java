package org.ray.streaming.runtime.config.worker;

import org.ray.streaming.runtime.config.Config;
import org.ray.streaming.runtime.generated.Streaming.OperatorType;

/**
 * Worker config.
 * note: Worker config is used for internal.
 */
public interface WorkerInternalConfig extends Config {

  String WORKER_NAME = "streaming.worker.name";
  String WORKER_TYPE = "streaming.worker.type";
  String OP_NAME = "streaming.worker.operator.name";
  String TASK_ID = "streaming.worker.task.id";

  @DefaultValue(value = "default-worker-name")
  @Key(value = WORKER_NAME)
  String workerName();

  @DefaultValue(value = "default-worker-type")
  @Key(value = WORKER_TYPE)
  OperatorType workerType();

  @DefaultValue(value = "default-worker-op-name")
  @Key(value = OP_NAME)
  String workerOperatorName();

  @DefaultValue(value = "default-worker-task-id")
  @Key(value = TASK_ID)
  String workerTaskId();
}
