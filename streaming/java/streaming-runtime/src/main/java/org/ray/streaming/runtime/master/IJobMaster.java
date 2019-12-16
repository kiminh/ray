package org.ray.streaming.runtime.master;

import com.alipay.streaming.runtime.decision.request.HotUpdateRequest;
import com.alipay.streaming.runtime.message.StmResult;
import com.alipay.streaming.runtime.monitor.event.RescaleEvent;

public interface IJobMaster {

  /**
   * Register job master context.
   * @return the boolean
   */
  Boolean registerContext(boolean isRecover);

  /**
   * Request job worker rollback, called by job worker.
   * @param requestBytes the request bytes
   * @return the boolean bytes
   */
  byte[] requestJobWorkerRollback(byte[] requestBytes);

  /**
   * Report job worker commit, called by job worker.
   * @param reportBytes the report bytes
   * @return the boolean bytes
   */
  byte[] reportJobWorkerCommit(byte[] reportBytes);

  /**
   * destroy
   */
  Boolean destroy();

  /**
   * get last checkpoint id
   *
   * @return last checkpoint id
   */
  Long getLastCheckpointId();

  /**
   * Report job worker initiative rollback, called by job worker.
   * @param reportBytes the report bytes
   * @return the boolean bytes
   */
  byte[] reportJobWorkerInitiativeRollback(byte[] reportBytes);

}