package org.ray.streaming.runtime.master;

import org.ray.api.RayActor;

public interface IJobMaster {

  /**
   * Init job master.
   * @param isRecover true: when in failover
   * @return
   */
  Boolean init(boolean isRecover);

  /**
   * Start all job workers.
   */
  void startAllWorkers();

  /**
   * Destroy all job workers.
   */
  Boolean destroyAllWorkers();
}