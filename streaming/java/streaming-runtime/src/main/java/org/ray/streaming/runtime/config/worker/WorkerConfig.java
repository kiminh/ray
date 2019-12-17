package org.ray.streaming.runtime.config.internal;

import org.aeonbits.owner.Config;

/**
 *
 */
public interface WorkerConfig extends Config {
  String WORKER_ID_INTERNAL = WorkerConfig.PY_WORKER_ID;
  String WORKER_NAME_INTERNAL = "StreamingWorkerName";
  String OPERATOR_NAME_INTERNAL = "StreamingOpName";
  String JOB_NAME_INTERNAL = "StreamingJobName";
  String RELIABILITY_LEVEL_INTERNAL = "reliability_level";
  String OPERATOR_TYPE_INTERNAL = "operator_type";
  String STATE_VERSION = "streaming.state.version";

  /**
   * The following key must be equal with StreamingConstants in python
   * package: python.streaming.runtime.core.constant
   * py: streaming_constants.py
   */
  String PY_CP_MODE = "save_checkpoint_mode";
  String PY_CP_MODE_PY = "save_checkpoint_mode_py";
  String PY_CP_STATE_BACKEND_TYPE = "cp_state_backend_type";
  String PY_CP_PANGU_CLUSTER_NAME = "cp_pangu_cluster_name";
  String PY_CP_PANGU_ROOT_DIR = "cp_pangu_root_dir";
  String PY_CP_PANGU_USER_MYSQL_URL = "cp_pangu_user_mysql_url";
  //  String PY_CP_LOCAL_DISK_ROOT_DIR = "cp_local_disk_root_dir";
  String PY_METRICS_TYPE = "metrics_type";
  String PY_METRICS_URL = "metrics_url";
  String PY_METRICS_USER_NAME = "metrics_user_name";
  String PY_RELIABILITY_LEVEL = "Reliability_Level";
  String PY_QUEUE_TYPE = "queue_type";
  String PY_QUEUE_SIZE = "queue_size";
  String PY_WORKER_ID = "worker_id";

  @DefaultValue(value = "default-worker-id")
  @Key(value = WORKER_ID_INTERNAL)
  String workerId();

  @DefaultValue(value = "default-worker-name")
  @Key(value = WORKER_NAME_INTERNAL)
  String workerName();

//  @DefaultValue(value = "source")
//  @Key(value = OPERATOR_TYPE_INTERNAL)
//  String operatorType();

  @DefaultValue(value = "default-operator-name")
  @Key(value = OPERATOR_NAME_INTERNAL)
  String operatorName();

  @DefaultValue(value = "")
  @Key(value = STATE_VERSION)
  String stateVersion();

}
