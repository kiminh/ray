package org.ray.streaming.runtime.master;

public interface IJobMaster {

  /**
   * Register job master context.
   * @return the boolean
   */
  Boolean registerContext(boolean isRecover);

  /**
   * Destroy job master.
   */
  Boolean destroy();
}